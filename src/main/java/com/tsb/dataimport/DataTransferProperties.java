package com.tsb.dataimport;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "datatransfer")
public class DataTransferProperties {
    /** 來源表 */
    private String sourceTable;
    /** 目標表清單 */
    private List<String> targetTables;
    /** 各 table 對應的欄位 mapping 設定 */
    private Map<String, String> columnMapping;

    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }

    public List<String> getTargetTables() { return targetTables; }
    public void setTargetTables(List<String> targetTables) { this.targetTables = targetTables; }

    public Map<String, String> getColumnMapping() { return columnMapping; }
    public void setColumnMapping(Map<String, String> columnMapping) { this.columnMapping = columnMapping; }
}