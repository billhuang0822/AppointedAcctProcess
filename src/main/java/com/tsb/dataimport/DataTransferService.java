package com.tsb.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

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

    public void runAll() {
        List<String> targetTables = props.getTargetTables();
        if (targetTables == null || targetTables.isEmpty()) {
            logger.warn("No target tables configured for data transfer.");
            return;
        }
        int pageSize = resolvePageSize();
        logger.info("Starting DataTransferService.runAll with targetTables={} pageSize={}", targetTables, pageSize);

        int offset = 0;
        boolean more = true;
        int processed = 0;

        // 分頁查詢
        String pageSql = "SELECT clna_cust_id_no, clna_bsb_no, clna_acct_no, clna_nom_acct_type, clna_last_maint_date, transfer_note " +
            "FROM (SELECT a.*, ROW_NUMBER() OVER (ORDER BY clna_cust_id_no, clna_bsb_no, clna_acct_no) rn FROM BANCS_MIN_CLNA a) " +
            "WHERE rn > ? AND rn <= ?";

        while (more) {
            try (
                Connection mainConn = mainDs.getConnection();
                Connection lookupConn = lookupDs.getConnection();
                PreparedStatement srcStmt = mainConn.prepareStatement(pageSql)
            ) {
                srcStmt.setInt(1, offset);
                srcStmt.setInt(2, offset + pageSize);
                int rowCount = 0;

                try (ResultSet rs = srcStmt.executeQuery();
                     PreparedStatement psTransfer = mainConn.prepareStatement(
                         "INSERT INTO " + getTargetName("TRANSFERACCOUNT") +
                         " (userid, brchid, accountno, updatedate, memo, email, receivermemo, synchancode) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
                     PreparedStatement psCustSet = mainConn.prepareStatement(
                         "INSERT INTO " + getTargetName("CUSTSETTRANSACCT") +
                         " (userid, useridtype, brchid, accountno, updatedate, memo, email, receivermemo) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                ) {
                    while (rs.next()) {
                        rowCount++;
                        processed++;
                        String clna_cust_id_no = trim(rs.getString("clna_cust_id_no"));
                        String clna_bsb_no = trim(rs.getString("clna_bsb_no"));
                        String clna_acct_no = trim(rs.getString("clna_acct_no"));
                        String clna_nom_acct_type = trim(rs.getString("clna_nom_acct_type"));
                        String clna_last_maint_date = trim(rs.getString("clna_last_maint_date"));
                        String transfer_note = trim(rs.getString("transfer_note"));

                        // lookup BANCS_SK_CUST_INFO
                        String ts_cust_id = null;
                        String cust_id_type = null;
                        try (PreparedStatement lookupStmt = lookupConn.prepareStatement(
                                "SELECT ts_cust_id, cust_id_type FROM BANCS_SK_CUST_INFO WHERE sk_cust_id = ?")) {
                            lookupStmt.setString(1, clna_cust_id_no);
                            try (ResultSet lrs = lookupStmt.executeQuery()) {
                                if (lrs.next()) {
                                    ts_cust_id = trim(lrs.getString("ts_cust_id"));
                                    cust_id_type = trim(lrs.getString("cust_id_type"));
                                }
                            }
                        }
                        if (ts_cust_id == null) continue;

                        // XREF 處理
                        String bsbUsed = clna_bsb_no;
                        String acctUsed = clna_acct_no;
                        if ("103".equals(clna_bsb_no)) {
                            try (PreparedStatement xrefStmt = mainConn.prepareStatement(
                                    "SELECT intn_ref_no FROM XREF WHERE extn_ref_no = ?")) {
                                xrefStmt.setString(1, clna_acct_no);
                                try (ResultSet xrs = xrefStmt.executeQuery()) {
                                    if (xrs.next()) {
                                        String intn = trim(xrs.getString("intn_ref_no"));
                                        if (intn != null) {
                                            acctUsed = intn;
                                            bsbUsed = "812";
                                        }
                                    }
                                }
                            }
                        }

                        if ("0".equals(clna_nom_acct_type) && targetTables.contains(getTargetName("TRANSFERACCOUNT"))) {
                            psTransfer.setString(1, ts_cust_id);
                            psTransfer.setString(2, bsbUsed);
                            psTransfer.setString(3, acctUsed);
                            psTransfer.setString(4, clna_last_maint_date);
                            psTransfer.setString(5, transfer_note);
                            psTransfer.setNull(6, Types.VARCHAR);
                            psTransfer.setNull(7, Types.VARCHAR);
                            psTransfer.setString(8, "RB");
                            psTransfer.addBatch();
                        } else if ("1".equals(clna_nom_acct_type) && targetTables.contains(getTargetName("CUSTSETTRANSACCT"))) {
                            psCustSet.setString(1, ts_cust_id);
                            psCustSet.setString(2, cust_id_type);
                            psCustSet.setString(3, bsbUsed);
                            psCustSet.setString(4, acctUsed);
                            psCustSet.setString(5, clna_last_maint_date);
                            psCustSet.setString(6, transfer_note);
                            psCustSet.setNull(7, Types.VARCHAR);
                            psCustSet.setNull(8, Types.VARCHAR);
                            psCustSet.addBatch();
                        }
                    } // end while rs

                    psTransfer.executeBatch();
                    psCustSet.executeBatch();
                    mainConn.commit();
                }
                if (rowCount < pageSize) more = false;
                offset += pageSize;
            } catch (SQLException e) {
                logger.error("Error during page offset {}: {}", offset, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        logger.info("DataTransfer finished: processed={}", processed);
    }

    private int resolvePageSize() {
        if (props.getPageSize() > 0) return props.getPageSize();
        if (props.getFetchSize() > 0) return props.getFetchSize();
        return 1000;
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