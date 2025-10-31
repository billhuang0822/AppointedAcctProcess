package com.tsb.dataimport;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "datatransfer")
public class DataTransferProperties {

    private int batchSize = 500;
    private int fetchSize = 1000;
    private int pageSize = 10000;
    private String xrefReplaceBsb = "812";
    private boolean updateIssyncAfterInsert = false;
    private boolean prepareTestData = false;

    // 新增：要複製的表清單
    private List<String> tablesToCopy;

    // 新增：測試表後綴
    private String testTableSuffix = "_TEST";

    // 其他原有欄位（略，請保留你的原本設定）

    private boolean prepareTestDataCopy = true;
    private int dataCopyLimit = 500;

    // getters / setters 只列出新增部分
    public List<String> getTablesToCopy() { return tablesToCopy; }
    public void setTablesToCopy(List<String> tablesToCopy) { this.tablesToCopy = tablesToCopy; }

    public String getTestTableSuffix() { return testTableSuffix; }
    public void setTestTableSuffix(String testTableSuffix) { this.testTableSuffix = testTableSuffix; }

    // ... 其他 getters/setters 請保留
}