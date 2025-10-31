package com.tsb.test;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

@ConfigurationProperties(prefix = "testtable")
public class TestTableProperties {
    /** 是否建立測試表 */
    private boolean prepareTestData = false;
    /** 複製前幾筆資料 */
    private int dataCopyLimit = 50;
    /** 要複製的表清單 */
    private List<String> tablesToCopy;
    /** 測試表的後綴字串 */
    private String testTableSuffix = "_TEST";

    public boolean isPrepareTestData() { return prepareTestData; }
    public void setPrepareTestData(boolean prepareTestData) { this.prepareTestData = prepareTestData; }

    public int getDataCopyLimit() { return dataCopyLimit; }
    public void setDataCopyLimit(int dataCopyLimit) { this.dataCopyLimit = dataCopyLimit; }

    public List<String> getTablesToCopy() { return tablesToCopy; }
    public void setTablesToCopy(List<String> tablesToCopy) { this.tablesToCopy = tablesToCopy; }

    public String getTestTableSuffix() { return testTableSuffix; }
    public void setTestTableSuffix(String testTableSuffix) { this.testTableSuffix = testTableSuffix; }
}