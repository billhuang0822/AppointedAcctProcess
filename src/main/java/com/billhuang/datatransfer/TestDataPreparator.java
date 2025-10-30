package com.billhuang.datatransfer;

import com.billhuang.datatransfer.config.DataTransferProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;

/**
 * TestDataPreparator - 改良版
 *
 * 行為：
 * - 若 datatransfer.prepare-test-data 為 false 則跳過
 * - 若 prepareTestDataCopy 為 true，會以 CTAS 建立 *_TEST 表 (CREATE TABLE dest AS SELECT * FROM src WHERE 1=0)
 * - 在建立之前，若目標 *_TEST 已存在 -> 先 DROP TABLE <dest> PURGE（避免殘留舊結構/資料），再建立
 * - 嘗試使用 DBMS_METADATA.GET_DEPENDENT_DDL 取得 INDEX/CONSTRAINT DDL 並改寫之後執行（若權限不足會記錄警告但不阻斷主要流程）
 * - 可設定 dataCopyLimit (複製前 N 筆資料到 *_TEST)
 *
 * 注意
 * - 執行 DROP / CREATE TABLE 需要對應權限。若無權限則會拋 SQLException（本程式會記錄並中止 prepare）
 * - 這個類別以 @Component 註冊，並透過建構子注入名為 "mainDataSource" 的 DataSource bean（如果你的 bean 名稱不同請調整 @Qualifier）
 */
@Component
public class TestDataPreparator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataPreparator.class);

    private final DataSource mainDs;
    private final DataTransferProperties props;

    public TestDataPreparator(@Qualifier("mainDataSource") DataSource mainDs, DataTransferProperties props) {
        this.mainDs = mainDs;
        this.props = props;
    }

    public void prepareIfNeeded() {
        if (!props.isPrepareTestData()) {
            logger.info("prepareTestData disabled; skipping test data preparation.");
            return;
        }

        logger.info("prepareTestData enabled - preparing test tables by copying existing tables.");
        try (Connection conn = mainDs.getConnection()) {
            conn.setAutoCommit(true);
            String schema = getSchema(conn);

            // copy tables (drop if exists then CTAS)
            copyTableReplaceIfExists(conn, schema, props.getTargetZero(), props.getTargetZero() + "_TEST");
            copyTableReplaceIfExists(conn, schema, props.getTargetOne(), props.getTargetOne() + "_TEST");
           

            // remap properties to test tables (use normalized names)
            props.setTargetZero(normalizeTestName(props.getTargetZero() + "_TEST"));
            props.setTargetOne(normalizeTestName(props.getTargetOne() + "_TEST"));
            
            logger.info("Test tables prepared and properties remapped to use *_TEST versions.");
        } catch (SQLException ex) {
            logger.error("Failed preparing test data tables", ex);
            throw new RuntimeException(ex);
        }
    }

    private String getSchema(Connection conn) throws SQLException {
        String schema = conn.getMetaData().getUserName();
        if (schema == null) schema = "";
        return schema.toUpperCase();
    }

    /**
     * If dest exists -> drop it, then perform CTAS and attempt to recreate dependent DDL.
     */
    private void copyTableReplaceIfExists(Connection conn, String schema, String srcTable, String destTable) throws SQLException {
        if (srcTable == null || srcTable.trim().isEmpty()) return;
        logger.info("Preparing copy for source {} -> dest {}", srcTable, destTable);

        // If dest exists, drop it first
        if (tableExists(conn, schema, destTable)) {
            logger.info("Destination table {} exists. Dropping before recreate.", destTable);
            dropTable(conn, destTable);
        }

        // 1) CTAS to copy structure
        if (props.isPrepareTestDataCopy()) {
            String ctasSql = "CREATE TABLE " + destTable + " AS SELECT * FROM " + srcTable + " WHERE 1=0";
            try (Statement s = conn.createStatement()) {
                logger.info("Executing CTAS: {}", ctasSql);
                s.execute(ctasSql);
            } catch (SQLException e) {
                logger.warn("CTAS failed for {}: {}. Aborting copy for this table.", srcTable, e.getMessage());
                throw e;
            }

            // 2) attempt to extract dependent DDL (indexes / constraints) via DBMS_METADATA
            try {
                // set session transform params to avoid storage/tablespace details
                try (Statement s = conn.createStatement()) {
                    s.execute("BEGIN " +
                            "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);" +
                            "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);" +
                            "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',FALSE);" +
                            "END;");
                } catch (SQLException se) {
                    logger.warn("Failed to set DBMS_METADATA transform params: {}", se.getMessage());
                }

                // INDEX DDL
                String depIndexSql = "SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DEPENDENT_DDL('INDEX','" + srcTable + "','" + schema + "'), 32767, 1) AS DDL FROM DUAL";
                try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(depIndexSql)) {
                    if (rs.next()) {
                        String ddl = rs.getString("DDL");
                        if (ddl != null && !ddl.trim().isEmpty()) {
                            executeDependentDdls(conn, ddl, srcTable, destTable);
                        }
                    }
                } catch (SQLException ex) {
                    logger.info("GET_DEPENDENT_DDL(INDEX) may be unsupported or returned nothing for {}: {}", srcTable, ex.getMessage());
                }

                // CONSTRAINT DDL
                String depConstrSql = "SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DEPENDENT_DDL('CONSTRAINT','" + srcTable + "','" + schema + "'), 32767, 1) AS DDL FROM DUAL";
                try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(depConstrSql)) {
                    if (rs.next()) {
                        String ddl = rs.getString("DDL");
                        if (ddl != null && !ddl.trim().isEmpty()) {
                            executeDependentDdls(conn, ddl, srcTable, destTable);
                        }
                    }
                } catch (SQLException ex) {
                    logger.info("GET_DEPENDENT_DDL(CONSTRAINT) may be unsupported or returned nothing for {}: {}", srcTable, ex.getMessage());
                }

                // optionally copy N rows
                int copyLimit = props.getDataCopyLimit();
                if (copyLimit > 0) {
                    String insertSql = "INSERT /*+ APPEND */ INTO " + destTable + " SELECT * FROM " + srcTable + " WHERE ROWNUM <= " + copyLimit;
                    try (Statement s = conn.createStatement()) {
                        int cnt = s.executeUpdate(insertSql);
                        logger.info("Copied {} rows from {} to {}", cnt, srcTable, destTable);
                    }
                }
            } catch (SQLException e) {
                logger.warn("Error while attempting to copy dependent DDL for {}: {}", srcTable, e.getMessage());
                // continue - CTAS succeeded, which is the most important part
            }
        } else {
            // If prepareTestDataCopy is false, do not CTAS; we still ensure dest does not exist
            logger.info("prepareTestDataCopy disabled; ensured destination {} is dropped and will not be created (no CTAS).", destTable);
        }
    }

    private void dropTable(Connection conn, String table) throws SQLException {
        String dropSql = "DROP TABLE " + table + " PURGE";
        try (Statement s = conn.createStatement()) {
            s.execute(dropSql);
            logger.info("Dropped table {}", table);
        } catch (SQLException ex) {
            // If drop failed, rethrow, since we don't want stale object to remain
            logger.error("Failed to drop table {}: {}", table, ex.getMessage());
            throw ex;
        }
    }

    private void executeDependentDdls(Connection conn, String ddlBlock, String srcTable, String destTable) {
        if (ddlBlock == null || ddlBlock.trim().isEmpty()) return;

        // Split statements; DBMS_METADATA may return many DDL statements concatenated.
        String[] statements = ddlBlock.split(";\n|;\r\n|;\r|\\\\n");
        for (String raw : statements) {
            String stmt = raw.trim();
            if (stmt.isEmpty()) continue;

            String modified = stmt.replaceAll("(?i)\"" + srcTable + "\"", "\"" + destTable + "\"");
            modified = modified.replaceAll("(?i)\\b" + srcTable + "\\b", destTable);

            // append suffix to index/constraint names to avoid name collisions
            modified = modified.replaceAll("(?i)(CREATE\\s+INDEX\\s+)(\\w+)", "$1$2_TEST");
            modified = modified.replaceAll("(?i)(CONSTRAINT\\s+)(\\w+)", "$1$2_TEST");

            try (Statement s = conn.createStatement()) {
                logger.info("Executing dependent DDL for {} -> {} : {}", srcTable, destTable, modified);
                s.execute(modified);
            } catch (SQLException ex) {
                logger.warn("Failed to execute dependent DDL (may be FK or invalid for test): {} ; error: {}", modified, ex.getMessage());
            }
        }
    }

    private boolean tableExists(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private String normalizeTestName(String name) {
        if (name == null) return null;
        // prevent double suffix _TEST_TEST etc.
        return name.replaceAll("(_TEST)+$", "_TEST");
    }
}