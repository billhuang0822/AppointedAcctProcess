package com.tsb.dataimport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

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
        List<String> targetTables = props.getTargetTables();
        if (targetTables == null || targetTables.isEmpty()) {
            logger.warn("No target tables configured for data transfer.");
            return;
        }
        int pageSize = resolvePageSize();

        logger.info("Starting DataTransferService.runAll with targetTables={} pageSize={}", targetTables, pageSize);

        try (Connection mainConn = mainDs.getConnection();
             Connection lookupConn = lookupDs.getConnection()) {

            mainConn.setAutoCommit(false);

            try (PreparedStatement lookupStmt = prepareLookupStmt(lookupConn);
                 PreparedStatement xrefStmt = prepareXrefStmt(mainConn);
                 PreparedStatement psTransfer = prepareInsertTransferAccount(mainConn);
                 PreparedStatement psCustSet = prepareInsertCustSetTransAcct(mainConn)) {

                processAllPages(mainConn, lookupStmt, xrefStmt, psTransfer, psCustSet, pageSize, targetTables);

            }
        }
    }

    // ===== Method 拆解區 =====

    private int resolvePageSize() {
        if (props.getPageSize() > 0) return props.getPageSize();
        if (props.getFetchSize() > 0) return props.getFetchSize();
        return 1000;
    }

    private PreparedStatement prepareLookupStmt(Connection lookupConn) throws SQLException {
        String sql = "SELECT ts_cust_id, cust_id_type FROM BANCS_SK_CUST_INFO WHERE sk_cust_id = ?";
        return lookupConn.prepareStatement(sql);
    }

    private PreparedStatement prepareXrefStmt(Connection mainConn) throws SQLException {
        String sql = "SELECT intn_ref_no FROM XREF WHERE extn_ref_no = ?";
        return mainConn.prepareStatement(sql);
    }

    private PreparedStatement prepareInsertTransferAccount(Connection mainConn) throws SQLException {
        String sql = "INSERT INTO " + getTargetName("TRANSFERACCOUNT") +
                " (userid, brchid, accountno, updatedate, memo, email, receivermemo, synchancode) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        return mainConn.prepareStatement(sql);
    }

    private PreparedStatement prepareInsertCustSetTransAcct(Connection mainConn) throws SQLException {
        String sql = "INSERT INTO " + getTargetName("CUSTSETTRANSACCT") +
                " (userid, useridtype, brchid, accountno, updatedate, memo, email, receivermemo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        return mainConn.prepareStatement(sql);
    }

    private void processAllPages(Connection mainConn,
                                 PreparedStatement lookupStmt,
                                 PreparedStatement xrefStmt,
                                 PreparedStatement psTransfer,
                                 PreparedStatement psCustSet,
                                 int pageSize,
                                 List<String> targetTables) throws SQLException {

        String pageSql = "SELECT clna_cust_id_no, clna_bsb_no, clna_acct_no, clna_nom_acct_type, clna_last_maint_date, transfer_note " +
                "FROM (SELECT a.*, ROW_NUMBER() OVER (ORDER BY clna_cust_id_no, clna_bsb_no, clna_acct_no) rn FROM BANCS_MIN_CLNA a) " +
                "WHERE rn > ? AND rn <= ?";

        int offset = 0;
        int processed = 0, insertedTransfer = 0, insertedCustSet = 0;
        boolean more = true;
        while (more) {
            try (PreparedStatement srcStmt = mainConn.prepareStatement(pageSql)) {
                srcStmt.setInt(1, offset);
                srcStmt.setInt(2, offset + pageSize);
                try (ResultSet rs = srcStmt.executeQuery()) {
                    int rowCount = 0;
                    while (rs.next()) {
                        rowCount++;
                        processed++;
                        DataRow row = extractDataRow(rs);

                        LookupInfo lookupInfo = doLookup(lookupStmt, row.clna_cust_id_no);
                        if (lookupInfo.ts_cust_id == null) continue;

                        XrefInfo xrefInfo = doXref(xrefStmt, row.clna_bsb_no, row.clna_acct_no);

                        if ("0".equals(row.clna_nom_acct_type) && targetTables.contains(getTargetName("TRANSFERACCOUNT"))) {
                            fillTransferAccount(psTransfer, lookupInfo, xrefInfo, row);
                            psTransfer.addBatch();
                            insertedTransfer++;
                        } else if ("1".equals(row.clna_nom_acct_type) && targetTables.contains(getTargetName("CUSTSETTRANSACCT"))) {
                            fillCustSetTransAcct(psCustSet, lookupInfo, xrefInfo, row);
                            psCustSet.addBatch();
                            insertedCustSet++;
                        }
                    } // while rs

                    // 批次送出
                    if (insertedTransfer > 0) {
                        psTransfer.executeBatch();
                        mainConn.commit();
                        insertedTransfer = 0;
                    }
                    if (insertedCustSet > 0) {
                        psCustSet.executeBatch();
                        mainConn.commit();
                        insertedCustSet = 0;
                    }

                    if (rowCount < pageSize) more = false;
                    offset += pageSize;
                }
            }
        }
        logger.info("DataTransfer finished: processed={}", processed);
    }

    // ====== Bean/Data Holder ======

    private static class DataRow {
        String clna_cust_id_no, clna_bsb_no, clna_acct_no, clna_nom_acct_type, clna_last_maint_date, transfer_note;
    }

    private static class LookupInfo {
        String ts_cust_id, cust_id_type;
    }

    private static class XrefInfo {
        String bsbUsed, acctUsed;
    }

    // ====== Extract/Lookup/Xref/Fill Methods ======

    private DataRow extractDataRow(ResultSet rs) throws SQLException {
        DataRow row = new DataRow();
        row.clna_cust_id_no = trim(rs.getString("clna_cust_id_no"));
        row.clna_bsb_no = trim(rs.getString("clna_bsb_no"));
        row.clna_acct_no = trim(rs.getString("clna_acct_no"));
        row.clna_nom_acct_type = trim(rs.getString("clna_nom_acct_type"));
        row.clna_last_maint_date = trim(rs.getString("clna_last_maint_date"));
        row.transfer_note = trim(rs.getString("transfer_note"));
        return row;
    }

    private LookupInfo doLookup(PreparedStatement lookupStmt, String clna_cust_id_no) throws SQLException {
        LookupInfo info = new LookupInfo();
        lookupStmt.setString(1, clna_cust_id_no);
        try (ResultSet lrs = lookupStmt.executeQuery()) {
            if (lrs.next()) {
                info.ts_cust_id = trim(lrs.getString("ts_cust_id"));
                info.cust_id_type = trim(lrs.getString("cust_id_type"));
            }
        }
        return info;
    }

    private XrefInfo doXref(PreparedStatement xrefStmt, String clna_bsb_no, String clna_acct_no) throws SQLException {
        XrefInfo info = new XrefInfo();
        info.bsbUsed = clna_bsb_no;
        info.acctUsed = clna_acct_no;
        if ("103".equals(clna_bsb_no)) {
            xrefStmt.setString(1, clna_acct_no);
            try (ResultSet xrs = xrefStmt.executeQuery()) {
                if (xrs.next()) {
                    String intn = trim(xrs.getString("intn_ref_no"));
                    if (intn != null) {
                        info.acctUsed = intn;
                        info.bsbUsed = "812";
                    }
                }
            }
        }
        return info;
    }

    private void fillTransferAccount(PreparedStatement ps, LookupInfo l, XrefInfo x, DataRow row) throws SQLException {
        ps.setString(1, l.ts_cust_id); // userid
        ps.setString(2, x.bsbUsed);    // brchid
        ps.setString(3, x.acctUsed);   // accountno
        ps.setString(4, row.clna_last_maint_date); // updatedate
        ps.setString(5, row.transfer_note); // memo
        ps.setNull(6, Types.VARCHAR); // email
        ps.setNull(7, Types.VARCHAR); // receivermemo
        ps.setString(8, "RB"); // synchancode
    }

    private void fillCustSetTransAcct(PreparedStatement ps, LookupInfo l, XrefInfo x, DataRow row) throws SQLException {
        ps.setString(1, l.ts_cust_id); // userid
        ps.setString(2, l.cust_id_type); // useridtype
        ps.setString(3, x.bsbUsed); // brchid
        ps.setString(4, x.acctUsed); // accountno
        ps.setString(5, row.clna_last_maint_date); // updatedate
        ps.setString(6, row.transfer_note); // memo
        ps.setNull(7, Types.VARCHAR); // email
        ps.setNull(8, Types.VARCHAR); // receivermemo
    }

    /** 取對應的目標表名稱（如 *_TEST） */
    private String getTargetName(String baseName) {
        for (String t : props.getTargetTables()) {
            if (t.equalsIgnoreCase(baseName) || t.toUpperCase().startsWith(baseName.toUpperCase() + "_")) {
                return t;
            }
        }
        return baseName;
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }
}