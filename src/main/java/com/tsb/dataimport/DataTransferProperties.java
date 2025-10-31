package com.tsb.dataimport;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datatransfer")
public class DataTransferProperties {

    // datasources are injected in Spring config, not here
    private int batchSize = 500;
    private int fetchSize = 1000;
    private int pageSize = 10000;
    private String xrefReplaceBsb = "812";
    private boolean updateIssyncAfterInsert = false;
    private boolean prepareTestData = false;

    // source table / columns
    private String sourceTable = "BANCS_MIN_CLNA";
    private String colClnaCustIdNo = "clna_cust_id_no";
    private String colClnaBsbNo = "clna_bsb_no";
    private String colClnaAcctNo = "clna_acct_no";
    private String colClnaNomAcctType = "clna_nom_acct_type";
    private String colClnaLastMaintDate = "clna_last_maint_date";
    private String colTransferNote = "transfer_note";
    private String colIssync = "issync";

    // lookup table
    private String lookupTable = "BANCS_SK_CUST_INFO";
    private String lookupSkCol = "sk_cust_id";
    private String lookupTsCol = "ts_cust_id";
    private String lookupCustIdTypeCol = "cust_id_type";

    // xref
    private String xrefTable = "XREF";
    private String xrefExtnCol = "extn_ref_no";
    private String xrefIntnCol = "intn_ref_no";

    // targets
    private String targetZero = "TRANSFERACCOUNT";
    private String targetOne = "CUSTSETTRANSACCT";

    // target key columns
    private String tgtUserid = "userid";
    private String tgtBrchid = "brchid";
    private String tgtAccountno = "accountno";

    // insert cols mapping CSV (can be overridden in application.yml for UT)
    private String insertZeroCols = "userid,brchid,accountno,updatedate,memo,email,receivermemo";
    private String insertZeroFrom = "ts_cust_id,CLNA_BSB_USED,CLNA_ACCT_USED,clna_last_maint_date,transfer_note,NULL,NULL";
    private String insertOneCols = "userid,useridtype,brchid,accountno,updatedate,memo,email,receivermemo";
    private String insertOneFrom = "ts_cust_id,cust_id_type,CLNA_BSB_USED,CLNA_ACCT_USED,clna_last_maint_date,transfer_note,NULL,NULL";

    
     // if true use CTAS + attempt to recreate indexes/constraints using DBMS_METADATA
    private boolean prepareTestDataCopy = true;
    // when copying data, limit rows copied (0 = no data copy)
    private int dataCopyLimit = 500;

    

    // getters / setters

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getFetchSize() { return fetchSize; }
    public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }
    
    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }


    public String getXrefReplaceBsb() { return xrefReplaceBsb; }
    public void setXrefReplaceBsb(String xrefReplaceBsb) { this.xrefReplaceBsb = xrefReplaceBsb; }

    public boolean isUpdateIssyncAfterInsert() { return updateIssyncAfterInsert; }
    public void setUpdateIssyncAfterInsert(boolean updateIssyncAfterInsert) { this.updateIssyncAfterInsert = updateIssyncAfterInsert; }

    public boolean isPrepareTestData() { return prepareTestData; }
    public void setPrepareTestData(boolean prepareTestData) { this.prepareTestData = prepareTestData; }

    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }

    public String getColClnaCustIdNo() { return colClnaCustIdNo; }
    public void setColClnaCustIdNo(String colClnaCustIdNo) { this.colClnaCustIdNo = colClnaCustIdNo; }

    public String getColClnaBsbNo() { return colClnaBsbNo; }
    public void setColClnaBsbNo(String colClnaBsbNo) { this.colClnaBsbNo = colClnaBsbNo; }

    public String getColClnaAcctNo() { return colClnaAcctNo; }
    public void setColClnaAcctNo(String colClnaAcctNo) { this.colClnaAcctNo = colClnaAcctNo; }

    public String getColClnaNomAcctType() { return colClnaNomAcctType; }
    public void setColClnaNomAcctType(String colClnaNomAcctType) { this.colClnaNomAcctType = colClnaNomAcctType; }

    public String getColClnaLastMaintDate() { return colClnaLastMaintDate; }
    public void setColClnaLastMaintDate(String colClnaLastMaintDate) { this.colClnaLastMaintDate = colClnaLastMaintDate; }

    public String getColTransferNote() { return colTransferNote; }
    public void setColTransferNote(String colTransferNote) { this.colTransferNote = colTransferNote; }

    public String getColIssync() { return colIssync; }
    public void setColIssync(String colIssync) { this.colIssync = colIssync; }

    public String getLookupTable() { return lookupTable; }
    public void setLookupTable(String lookupTable) { this.lookupTable = lookupTable; }

    public String getLookupSkCol() { return lookupSkCol; }
    public void setLookupSkCol(String lookupSkCol) { this.lookupSkCol = lookupSkCol; }

    public String getLookupTsCol() { return lookupTsCol; }
    public void setLookupTsCol(String lookupTsCol) { this.lookupTsCol = lookupTsCol; }

    public String getLookupCustIdTypeCol() { return lookupCustIdTypeCol; }
    public void setLookupCustIdTypeCol(String lookupCustIdTypeCol) { this.lookupCustIdTypeCol = lookupCustIdTypeCol; }

    public String getXrefTable() { return xrefTable; }
    public void setXrefTable(String xrefTable) { this.xrefTable = xrefTable; }

    public String getXrefExtnCol() { return xrefExtnCol; }
    public void setXrefExtnCol(String xrefExtnCol) { this.xrefExtnCol = xrefExtnCol; }

    public String getXrefIntnCol() { return xrefIntnCol; }
    public void setXrefIntnCol(String xrefIntnCol) { this.xrefIntnCol = xrefIntnCol; }

    public String getTargetZero() { return targetZero; }
    public void setTargetZero(String targetZero) { this.targetZero = targetZero; }

    public String getTargetOne() { return targetOne; }
    public void setTargetOne(String targetOne) { this.targetOne = targetOne; }

    public String getTgtUserid() { return tgtUserid; }
    public void setTgtUserid(String tgtUserid) { this.tgtUserid = tgtUserid; }

    public String getTgtBrchid() { return tgtBrchid; }
    public void setTgtBrchid(String tgtBrchid) { this.tgtBrchid = tgtBrchid; }

    public String getTgtAccountno() { return tgtAccountno; }
    public void setTgtAccountno(String tgtAccountno) { this.tgtAccountno = tgtAccountno; }

    public String getInsertZeroCols() { return insertZeroCols; }
    public void setInsertZeroCols(String insertZeroCols) { this.insertZeroCols = insertZeroCols; }

    public String getInsertZeroFrom() { return insertZeroFrom; }
    public void setInsertZeroFrom(String insertZeroFrom) { this.insertZeroFrom = insertZeroFrom; }

    public String getInsertOneCols() { return insertOneCols; }
    public void setInsertOneCols(String insertOneCols) { this.insertOneCols = insertOneCols; }

    public String getInsertOneFrom() { return insertOneFrom; }
    public void setInsertOneFrom(String insertOneFrom) { this.insertOneFrom = insertOneFrom; }
    
    public boolean isPrepareTestDataCopy() { return prepareTestDataCopy; }
    public void setPrepareTestDataCopy(boolean prepareTestDataCopy) { this.prepareTestDataCopy = prepareTestDataCopy; }

    public int getDataCopyLimit() { return dataCopyLimit; }
    public void setDataCopyLimit(int dataCopyLimit) { this.dataCopyLimit = dataCopyLimit; }

}