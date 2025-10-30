package com.billhuang.datatransfer;

import com.billhuang.datatransfer.config.DataTransferProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * TestDataPreparator - 在 prepareTestData = true 時建立測試用表（若不存在）
 *
 * 這是簡單實作：如果你要複製 production 結構/資料，請把更完整的 CTAS 或 DDL copy 邏輯放在此處。
 */
@Component
public class TestDataPreparator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataPreparator.class);

    private final DataSource mainDs;
    private final DataTransferProperties props;

    public TestDataPreparator(DataSource mainDs, DataTransferProperties props) {
        this.mainDs = mainDs;
        this.props = props;
    }

    public void prepareIfNeeded() {
        if (!props.isPrepareTestData()) {
            logger.info("prepareTestData disabled by config; skipping test data preparation.");
            return;
        }

        logger.info("prepareTestData enabled - preparing test tables (non-destructive placeholders).");
        // Example: create simple empty test tables with safe names (TARGETNAME_TEST)
        try (Connection conn = mainDs.getConnection();
             Statement s = conn.createStatement()) {

            String t0 = props.getTargetZero() + "_TEST";
            String t1 = props.getTargetOne() + "_TEST";

            // create basic structures if not exist (Oracle: no IF NOT EXISTS -> catch exception)
            try {
                s.execute("CREATE TABLE " + t0 + " (userid VARCHAR2(20), brchid VARCHAR2(4), accountno VARCHAR2(20))");
                logger.info("Created table {}", t0);
            } catch (SQLException e) {
                logger.info("Table {} may already exist: {}", t0, e.getMessage());
            }

            try {
                s.execute("CREATE TABLE " + t1 + " (userid VARCHAR2(20), useridtype VARCHAR2(2), brchid VARCHAR2(3), accountno VARCHAR2(20))");
                logger.info("Created table {}", t1);
            } catch (SQLException e) {
                logger.info("Table {} may already exist: {}", t1, e.getMessage());
            }

            // After creating, change properties to point to these test tables for safe UT usage
            props.setTargetZero(t0);
            props.setTargetOne(t1);

            logger.info("Test tables prepared and properties updated to use test tables.");
        } catch (SQLException ex) {
            logger.error("Failed to prepare test data tables", ex);
            throw new RuntimeException(ex);
        }
    }
}