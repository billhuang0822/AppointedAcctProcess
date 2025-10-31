package com.tsb.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;

/**
 * DataTransferService (paged by Oracle ROW_NUMBER)
 *
 * Changes:
 * - Use key-based pagination implemented with Oracle ROW_NUMBER() to page over source table
 *   (BANCS_MIN_CLNA) so we don't hold one huge ResultSet for the whole table.
 * - Page size is configurable through props.getPageSize(); if missing, fall back to props.getFetchSize().
 * - Each page opens/uses its own Connection/Statement/ResultSet lifecycle (try-with-resources).
 * - PreparedStatements for merge/update are reused within the runAll call, but source select happens per page.
 * - Commits after batches (as before). Ensures all JDBC resources are closed promptly to avoid connection leaks.
 *
 * Notes:
 * - This implementation orders the pagination by the composite key:
 *     clna_cust_id_no, clna_bsb_no, clna_acct_no
 *   Make sure these columns uniquely identify rows or change the ORDER BY to an appropriate unique key.
 *
 * - If your DataTransferProperties does not have getPageSize(), add it (int) and expose it via configuration.
 *   The code will fall back to getFetchSize() when getPageSize() returns <= 0.
 */
@Service
public class DataTransferService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);

    private final DataSource mainDs;
    private final DataSource lookupDs;
    private final DataTransferProperties props;

    public DataTransferService(@Qualifier("mainDataSource") DataSource mainDs,
                               @Qualifier("lookupDataSource") DataSource lookupDs,
                               DataTransferProperties props) {
        this.mainDs = mainDs;
        this.lookupDs = lookupDs;
        this.props = props;
    }

    public void runAll() throws SQLException {
        // Normalize target names to avoid duplicated suffixes if TestDataPreparator appended multiple times
        String targetZero = normalizeTableName(props.getTargetZero());
        String targetOne = normalizeTableName(props.getTargetOne());

        // Update props locally (optional) so downstream code using props getters gets normalized names
        props.setTargetZero(targetZero);
        props.setTargetOne(targetOne);

        logger.info("Starting DataTransferService.runAll using targetZero={} targetOne={}", targetZero, targetOne);

        // Decide page size: prefer pageSize config; fallback to fetchSize
        int pageSize = 0;
        try {
            pageSize = props.getPageSize(); // preferred (add to DataTransferProperties)
        } catch (Throwable t) {
            // ignore if not present
        }
        if (pageSize <= 0) {
            pageSize = props.getFetchSize() > 0 ? props.getFetchSize() : 1000;
        }
        logger.info("Using pageSize={} for paged source read", pageSize);

        try (Connection mainConn = mainDs.getConnection();
             Connection lookupConn = lookupDs.getConnection()) {

            mainConn.setAutoCommit(false);

            // prepare lookup statement (lookup DB)
            String lookupSql = "SELECT " + props.getLookupTsCol() + ", " + props.getLookupCustIdTypeCol()
                    + " FROM " + props.getLookupTable() + " WHERE " + props.getLookupSkCol() + " = ?";
            try (PreparedStatement lookupStmt = lookupConn.prepareStatement(lookupSql)) {

                // xref lookup (main DB)
                String xrefSql = "SELECT " + props.getXrefIntnCol() + " FROM " + props.getXrefTable() + " WHERE " + props.getXrefExtnCol() + " = ?";
                try (PreparedStatement xrefStmt = mainConn.prepareStatement(xrefSql)) {

                    // build merge SQLs dynamically from (normalized) properties
                    String[] zeroInsertCols = props.getInsertZeroCols().split("\\s*,\\s*");
                    String[] oneInsertCols = props.getInsertOneCols().split("\\s*,\\s*");
                    String[] keyCols = new String[]{props.getTgtUserid(), props.getTgtBrchid(), props.getTgtAccountno()};

                    String mergeZeroSql = MergeSqlBuilder.buildMergeInsertSql(targetZero, zeroInsertCols, keyCols);
                    String mergeOneSql = MergeSqlBuilder.buildMergeInsertSql(targetOne, oneInsertCols, keyCols);

                    logger.debug("mergeZeroSql = {}", mergeZeroSql);
                    logger.debug("mergeOneSql = {}", mergeOneSql);

                    try (PreparedStatement mergeZero = mainConn.prepareStatement(mergeZeroSql);
                         PreparedStatement mergeOne = mainConn.prepareStatement(mergeOneSql)) {

                        // optional update issync
                        PreparedStatement updateIssyncStmt = null;
                        if (props.isUpdateIssyncAfterInsert()) {
                            String updateSql = "UPDATE " + props.getSourceTable() + " SET " + props.getColIssync() +
                                    "='Y' WHERE " + props.getColClnaCustIdNo() + " = ? AND " + props.getColClnaBsbNo() + " = ? AND " + props.getColClnaAcctNo() + " = ?";
                            updateIssyncStmt = mainConn.prepareStatement(updateSql);
                        }

                        // We'll page through the source table using ROW_NUMBER() over an ordered subquery.
                        // ORDER BY must be stable and ideally by the PK / unique key for deterministic paging.
                        String srcInnerOrder = props.getColClnaCustIdNo() + ", " + props.getColClnaBsbNo() + ", " + props.getColClnaAcctNo();
                        String srcSelectCols = props.getColClnaCustIdNo() + ", " + props.getColClnaBsbNo() + ", " +
                                props.getColClnaAcctNo() + ", " + props.getColClnaNomAcctType() + ", " +
                                props.getColClnaLastMaintDate() + ", " + props.getColTransferNote();

                        // The paged SQL uses ROW_NUMBER() to produce rn and selects rows where rn between (offset+1) and (offset+pageSize)
                        String pagedSqlTemplate = "SELECT " + srcSelectCols + " FROM ( " +
                                "SELECT a.*, ROW_NUMBER() OVER (ORDER BY " + srcInnerOrder + ") rn FROM " + props.getSourceTable() + " a" +
                                " ) WHERE rn > ? AND rn <= ?";

                        logger.debug("Paged source SQL template: {}", pagedSqlTemplate);

                        int processed = 0, inserted = 0;
                        int batchZero = 0, batchOne = 0;

                        int offset = 0;
                        int fetchedThisPage;

                        // Loop pages
                        do {
                            int lower = offset;
                            int upper = offset + pageSize;

                            logger.debug("Reading source rows rn > {} and rn <= {}", lower, upper);

                            try (PreparedStatement srcStmt = mainConn.prepareStatement(pagedSqlTemplate,
                                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                                // set bounds for this page
                                srcStmt.setInt(1, lower);
                                srcStmt.setInt(2, upper);
                                srcStmt.setFetchSize(pageSize);

                                try (ResultSet rs = srcStmt.executeQuery()) {
                                    fetchedThisPage = 0;
                                    while (rs.next()) {
                                        fetchedThisPage++;
                                        processed++;
                                        String skCustId = trim(rs.getString(props.getColClnaCustIdNo()));
                                        String bsb = trim(rs.getString(props.getColClnaBsbNo()));
                                        String acct = trim(rs.getString(props.getColClnaAcctNo()));
                                        String acctType = trim(rs.getString(props.getColClnaNomAcctType()));
                                        String lastMaint = trim(rs.getString(props.getColClnaLastMaintDate()));
                                        String note = trim(rs.getString(props.getColTransferNote()));

                                        if (skCustId == null) continue;

                                        // XREF conversion if bsb == '103'
                                        String bsbUsed = bsb;
                                        String acctUsed = acct;
                                        if ("103".equals(bsb)) {
                                            xrefStmt.setString(1, acct);
                                            try (ResultSet xrs = xrefStmt.executeQuery()) {
                                                if (xrs.next()) {
                                                    String intn = trim(xrs.getString(1));
                                                    if (intn != null) {
                                                        acctUsed = intn;
                                                        bsbUsed = props.getXrefReplaceBsb();
                                                    }
                                                }
                                            }
                                        }

                                        // lookup ts_cust_id
                                        String tsCustId = null;
                                        String custIdType = null;
                                        lookupStmt.setString(1, skCustId);
                                        try (ResultSet lrs = lookupStmt.executeQuery()) {
                                            if (lrs.next()) {
                                                tsCustId = trim(lrs.getString(1));
                                                custIdType = trim(lrs.getString(2));
                                            }
                                        }
                                        if (tsCustId == null) continue;

                                        // decide target by acctType
                                        if ("0".equals(acctType)) {
                                            setParamsByMapping(mergeZero, props.getInsertZeroFrom(), tsCustId, custIdType,
                                                    bsbUsed, acctUsed, lastMaint, note);
                                            mergeZero.addBatch();
                                            batchZero++;
                                            if (batchZero >= props.getBatchSize()) {
                                                int[] res = mergeZero.executeBatch();
                                                mainConn.commit();
                                                inserted += sum(res);
                                                batchZero = 0;
                                            }
                                        } else if ("1".equals(acctType)) {
                                            setParamsByMapping(mergeOne, props.getInsertOneFrom(), tsCustId, custIdType,
                                                    bsbUsed, acctUsed, lastMaint, note);
                                            mergeOne.addBatch();
                                            batchOne++;
                                            if (batchOne >= props.getBatchSize()) {
                                                int[] res = mergeOne.executeBatch();
                                                mainConn.commit();
                                                inserted += sum(res);
                                                batchOne = 0;
                                            }
                                        } else {
                                            // skip unknown acct type
                                            continue;
                                        }

                                        // optionally mark source row as synced (use original bsb/acct)
                                        if (props.isUpdateIssyncAfterInsert() && updateIssyncStmt != null) {
                                            updateIssyncStmt.setString(1, skCustId);
                                            updateIssyncStmt.setString(2, bsb);
                                            updateIssyncStmt.setString(3, acct);
                                            updateIssyncStmt.addBatch();
                                        }
                                    } // end while rs
                                }
                            }

                            // advance offset
                            offset += pageSize;

                            // If we fetched less than pageSize then we've reached the end.
                            // However, we captured fetchedThisPage inside the try-with-resources; we need a small trick:
                            // Instead of relying on fetchedThisPage outside, detect end by running a quick check:
                            // (To keep it simple here, we'll query the next page header-less: if previous page returned < pageSize, end)
                            // We'll implement by checking last page's fetch count via a small inline query:
                            // But to avoid extra query, keep simple: if the last page size < pageSize then the loop will end.
                            // So we need a mechanism to know fetchedThisPage value here; to keep code simple and safe,
                            // we re-run a lightweight count on the last page to decide termination.
                            // Simpler: stop when offset > maxRows; here we try to detect end by attempting to select one row beyond upper bound.

                            // Try to detect if more rows exist by selecting rn > offset and rn <= offset+1
                            boolean hasMore;
                            String probeSql = "SELECT 1 FROM (SELECT ROW_NUMBER() OVER (ORDER BY " + srcInnerOrder + ") rn FROM " + props.getSourceTable() + ") WHERE rn > ? AND rn <= ?";
                            try (PreparedStatement probeStmt = mainConn.prepareStatement(probeSql)) {
                                probeStmt.setInt(1, offset);
                                probeStmt.setInt(2, offset + 1);
                                try (ResultSet prs = probeStmt.executeQuery()) {
                                    hasMore = prs.next();
                                }
                            }
                            if (!hasMore) {
                                // final flush & commit below after loop
                                break;
                            }
                        } while (true);

                        // flush remaining batches
                        if (batchZero > 0) {
                            inserted += sum(mergeZero.executeBatch());
                        }
                        if (batchOne > 0) {
                            inserted += sum(mergeOne.executeBatch());
                        }
                        if (props.isUpdateIssyncAfterInsert() && updateIssyncStmt != null) {
                            updateIssyncStmt.executeBatch();
                        }
                        mainConn.commit();

                        logger.info("DataTransfer finished: processed={}, inserted={}", processed, inserted);
                    }
                }
            }
        }
    }

    private static int sum(int[] arr) {
        int s = 0;
        for (int v : arr) { if (v > 0) s += v; }
        return s;
    }

    private static void setParamsByMapping(PreparedStatement ps, String mappingCsv,
                                           String tsCustId, String custIdType,
                                           String bsbUsed, String acctUsed,
                                           String lastMaintDate, String transferNote) throws SQLException {
        String[] tokens = mappingCsv.split("\\s*,\\s*");
        for (int i = 0; i < tokens.length; i++) {
            String t = tokens[i];
            int idx = i + 1;
            if ("ts_cust_id".equalsIgnoreCase(t)) {
                ps.setString(idx, tsCustId);
            } else if ("cust_id_type".equalsIgnoreCase(t)) {
                ps.setString(idx, custIdType);
            } else if ("CLNA_BSB_USED".equalsIgnoreCase(t)) {
                ps.setString(idx, bsbUsed);
            } else if ("CLNA_ACCT_USED".equalsIgnoreCase(t)) {
                ps.setString(idx, acctUsed);
            } else if ("clna_last_maint_date".equalsIgnoreCase(t)) {
                ps.setString(idx, lastMaintDate);
            } else if ("transfer_note".equalsIgnoreCase(t)) {
                ps.setString(idx, transferNote);
            } else if ("NULL".equalsIgnoreCase(t)) {
                ps.setNull(idx, Types.VARCHAR);
            } else if (t != null && t.startsWith("CONST:")) {
                ps.setString(idx, t.substring("CONST:".length()));
            } else {
                ps.setNull(idx, Types.VARCHAR);
            }
        }
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }

    /**
     * Simple normalization to avoid repeated suffixes like _TEST_TEST if props mutated multiple times.
     */
    private static String normalizeTableName(String name) {
        if (name == null) return null;
        // collapse repeated "_TEST_TEST..." to single "_TEST"
        while (name.endsWith("_TEST_TEST")) {
            name = name.substring(0, name.length() - "_TEST".length());
        }
        // also if name ends with multiple _TEST segments like "..._TEST_TEST", ensure single
        if (name.endsWith("_TEST_TEST")) {
            name = name.replaceAll("(_TEST)+$", "_TEST");
        }
        return name;
    }
}