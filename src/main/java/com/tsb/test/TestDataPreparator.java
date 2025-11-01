package com.tsb.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.tsb.dataimport.DataTransferProperties;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 自動複製設定的資料表（含結構/資料）為測試用表
 */
@Component
public class TestDataPreparator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataPreparator.class);

    private final DataSource mainDs;
    private final TestTableProperties testProps;
    private final DataTransferProperties transferProps;

    public TestDataPreparator(
            @Qualifier("mainDataSource") DataSource mainDs,
            TestTableProperties testProps,
            DataTransferProperties transferProps) {
        this.mainDs = mainDs;
        this.testProps = testProps;
        this.transferProps = transferProps;
    }

    public void prepareTestTables() {
        if (!testProps.isPrepareTestData()) {
            logger.info("Test data preparation disabled, skipping.");
            return;
        }
        List<String> tables = testProps.getTablesToCopy();
        if (tables == null || tables.isEmpty()) {
            logger.warn("No tables configured for test data preparation.");
            return;
        }
        String suffix = testProps.getTestTableSuffix();
        try (Connection conn = mainDs.getConnection()) {
            String schema = getSchema(conn);
            for (String srcTable : tables) {
                if (srcTable != null && !srcTable.isBlank()) {
                    copyTableWithData(conn, schema, srcTable, suffix);
                }
            }
            logger.info("Test tables prepared: {} with suffix '{}'", tables, suffix);

            // 這裡自動同步修改資料轉換目標表為測試表
            List<String> testTargetTables = tables.stream()
                    .map(t -> normalizeTestName(t + suffix, suffix))
                    .collect(Collectors.toList());
            transferProps.setTargetTables(testTargetTables);
            logger.info("DataTransferProperties.targetTables updated to {}", testTargetTables);

        } catch (SQLException ex) {
            logger.error("Error preparing test tables", ex);
            throw new RuntimeException(ex);
        }
    }

    private String getSchema(Connection conn) throws SQLException {
        String schema = conn.getMetaData().getUserName();
        return schema == null ? "" : schema.toUpperCase();
    }

    private void copyTableWithData(Connection conn, String schema, String srcTable, String suffix) throws SQLException {
        String destTable = normalizeTestName(srcTable + suffix, suffix);

        dropIfExists(conn, schema, destTable);
        createEmptyCopy(conn, srcTable, destTable);
        copyIndexes(conn, schema, srcTable, destTable, suffix);
        copyConstraints(conn, schema, srcTable, destTable, suffix);
        copyTableRows(conn, srcTable, destTable, testProps.getDataCopyLimit());
    }

    private void dropIfExists(Connection conn, String schema, String table) throws SQLException {
        if (tableExists(conn, schema, table)) {
            String sql = "DROP TABLE " + table + " PURGE";
            try (Statement s = conn.createStatement()) {
                s.execute(sql);
                logger.info("Dropped existing table {}", table);
            }
        }
    }

    private void createEmptyCopy(Connection conn, String srcTable, String destTable) throws SQLException {
        String sql = "CREATE TABLE " + destTable + " AS SELECT * FROM " + srcTable + " WHERE 1=0";
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            logger.info("Created table {} (structure copied from {})", destTable, srcTable);
        }
    }

    private void copyIndexes(Connection conn, String schema, String srcTable, String destTable, String suffix) {
        String sql = "SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DEPENDENT_DDL('INDEX', ?, ?), 32767, 1) AS DDL FROM DUAL";
        copyDependentDdl(conn, sql, schema, srcTable, destTable, "INDEX", suffix);
    }

    private void copyConstraints(Connection conn, String schema, String srcTable, String destTable, String suffix) {
        String sql = "SELECT DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DEPENDENT_DDL('CONSTRAINT', ?, ?), 32767, 1) AS DDL FROM DUAL";
        copyDependentDdl(conn, sql, schema, srcTable, destTable, "CONSTRAINT", suffix);
    }

    private void copyDependentDdl(Connection conn, String sql, String schema, String srcTable, String destTable, String type, String suffix) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, srcTable.toUpperCase());
            ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String ddl = rs.getString("DDL");
                    if (ddl != null && !ddl.isBlank()) {
                        for (String stmt : ddl.split(";\n|;\r\n|;\r|\\n")) {
                            String modStmt = stmt
                                    .replaceAll("(?i)\"" + srcTable + "\"", "\"" + destTable + "\"")
                                    .replaceAll("(?i)\\b" + srcTable + "\\b", destTable)
                                    .replaceAll("(?i)(CREATE\\s+" + type + "\\s+)(\\w+)", "$1$2" + suffix)
                                    .replaceAll("(?i)(CONSTRAINT\\s+)(\\w+)", "$1$2" + suffix);
                            if (!modStmt.isBlank()) {
                                try (Statement s = conn.createStatement()) {
                                    s.execute(modStmt);
                                } catch (SQLException ex) {
                                    logger.warn("Skip dependent DDL: {} (error: {})", modStmt, ex.getMessage());
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            logger.info("Skip {} DDL copy for {}: {}", type, srcTable, ex.getMessage());
        }
    }

    private void copyTableRows(Connection conn, String srcTable, String destTable, int limit) {
        if (limit <= 0) return;
        String sql = String.format("INSERT INTO %s SELECT * FROM %s WHERE ROWNUM <= %d", destTable, srcTable, limit);
        try (Statement s = conn.createStatement()) {
            int rows = s.executeUpdate(sql);
            logger.info("Copied {} rows from {} to {}", rows, srcTable, destTable);
        } catch (SQLException ex) {
            logger.warn("Copy rows failed for {}: {}", srcTable, ex.getMessage());
        }
    }

    private boolean tableExists(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private String normalizeTestName(String name, String suffix) {
        if (name == null) return null;
        return name.replaceAll("(" + suffix + ")+$", suffix);
    }
}