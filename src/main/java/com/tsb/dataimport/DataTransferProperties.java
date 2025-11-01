package com.tsb.dataimport;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "datatransfer")
public class DataTransferProperties {

    // 分頁/批次等效能設定
    private int batchSize = 500;
    private int fetchSize = 1000;
    private int pageSize = 1000;

    // 來源表
    private String sourceTable = "BANCS_MIN_CLNA";

    // 目標表，支援多個（會自動覆蓋為 *_TEST 由 TestDataPreparator 控制）
    private List<String> targetTables;

    
    // === getter/setter ===

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getFetchSize() { return fetchSize; }
    public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }

    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }

    public List<String> getTargetTables() { return targetTables; }
    public void setTargetTables(List<String> targetTables) { this.targetTables = targetTables; }

}