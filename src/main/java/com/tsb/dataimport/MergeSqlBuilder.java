package com.tsb.dataimport;

/**
 * MergeSqlBuilder - 建構 Oracle MERGE INTO SQL 模板。
 *
 * 用法：
 *   String sql = MergeSqlBuilder.buildMergeInsertSql(
 *       table, insertColsArray, keyColsArray);
 *
 * PreparedStatement 的 bind 參數順序必須跟 insertColsArray 一致。
 */
public class MergeSqlBuilder {

    public static String buildMergeInsertSql(String table, String[] insertCols, String[] keyCols) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(table).append(" tgt\n")
          .append("USING (SELECT ");

        for (int i = 0; i < insertCols.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append("? AS ").append(insertCols[i]);
        }
        sb.append(" FROM DUAL) src\nON (");

        for (int i = 0; i < keyCols.length; i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("tgt.").append(keyCols[i]).append(" = src.").append(keyCols[i]);
        }
        sb.append(")\nWHEN NOT MATCHED THEN\nINSERT (");

        for (int i = 0; i < insertCols.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(insertCols[i]);
        }
        sb.append(")\nVALUES (");
        for (int i = 0; i < insertCols.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append("src.").append(insertCols[i]);
        }
        sb.append(")");

        return sb.toString();
    }
}