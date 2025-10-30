package com.billhuang.datatransfer.service;

import com.billhuang.datatransfer.config.DataTransferProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;

/**
 * DataTransferService (fixed)
 *
 * - 不在 constructor 或 @PostConstruct 建立 PreparedStatement
 * - 在 runAll() 裡根據最新 props 動態建 SQL / PreparedStatement
 * - 對 table name 做簡單 normalize（避免 _TEST 被重複附加）
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
       
        logger.info("Starting DataTransferService.runAll using targetZero={} targetOne={}",
                targetZero, targetOne);

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

                        // source SELECT
                        String srcSql = "SELECT " + props.getColClnaCustIdNo() + ", " + props.getColClnaBsbNo() + ", " +
                                props.getColClnaAcctNo() + ", " + props.getColClnaNomAcctType() + ", " +
                                props.getColClnaLastMaintDate() + ", " + props.getColTransferNote() +
                                " FROM " + props.getSourceTable();

                        try (Statement srcStmt = mainConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            srcStmt.setFetchSize(props.getFetchSize());
                            try (ResultSet rs = srcStmt.executeQuery(srcSql)) {
                                rs.setFetchSize(props.getFetchSize());

                                int batchZero = 0, batchOne = 0;
                                int processed = 0, inserted = 0;

                                while (rs.next()) {
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
                                } // end while

                                if (batchZero > 0) { inserted += sum(mergeZero.executeBatch()); }
                                if (batchOne > 0) { inserted += sum(mergeOne.executeBatch()); }
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