package rtaclient;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;


import rtaclient.common.GlobalEnv;
import rtaclient.common.Log;
import rtaclient.db.DBConnect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class QueryExecutor {

    // tmpDBが存在していなかったら作成
    public static void createTmp() throws SQLException {
        Connection con = DBConnect.connectLocal();
        String tmpDB = GlobalEnv.getTmpdb();
        String dbms = GlobalEnv.getDriver();
        String sql = "";
        switch (dbms) {
            case "mysql":
                sql = "CREATE DATABASE IF NOT EXISTS " + tmpDB + " CHARACTER SET utf8";
                break;

            case "postgresql":
                sql = "CREATE SCHEMA IF NOT EXISTS " + tmpDB;
                break;

            case "sqlite":
                // do nothing
                return;

            default:
                System.out.println("Sorry, " + dbms + " is not supported yet.");
        }
        PreparedStatement ps = con.prepareStatement(sql);
        ps.executeUpdate();
        DBConnect.close(con);
    }

    public static void insertFromJson(String dbms, String rs_json, String tmpdate, Boolean original) throws SQLException {

        Connection con = null;
        // psqlの場合は同一dbms内で別schemaに繋ぐため
        switch (GlobalEnv.getDriver()) {
            case "mysql":
                con = DBConnect.connectLocalTmp();
                break;
            case "postgresql":
                con = DBConnect.connectLocal();
                break;
        }

        PreparedStatement ps;

        // JSONの解析
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(rs_json);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String tableName = root.get("table_name").asText();
        root.get("table_name").fieldNames();

        // SQL前半部
        String createSQL = "CREATE TABLE ";
        String insertSQL = "INSERT INTO ";

        switch (GlobalEnv.getDriver()) {
            case "mysql":
                if (original) {
                    createSQL += "result_" + tmpdate + " (";
                    insertSQL += "result_" + tmpdate + " (";
                } else {
                    createSQL += tableName + "_" + tmpdate + " (";
                    insertSQL += tableName + "_" + tmpdate + " (";
                }

                break;
            case "postgresql":
            case "sqlite":
                if (original) {
                    createSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
                    insertSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
                } else {
                    createSQL += GlobalEnv.getTmpdb() + "." + tableName + "_" + tmpdate + " (";
                    insertSQL += GlobalEnv.getTmpdb() + "." + tableName + "_" + tmpdate + " (";
                }

                break;

            default:
                break;
        }

        Iterator<Map.Entry<String, JsonNode>> metaFields = root.get("metadata").fields();
        while (metaFields.hasNext()) {
            Map.Entry<String, JsonNode> metaField = metaFields.next();
            // TODO: DBMSで分岐
            Log.out(metaField.getValue().asText());
            switch (metaField.getValue().asText()) {
                case "int":
                case "int4":
                    createSQL += metaField.getKey() + " int, ";
                    break;
                case "bigint":
                case "int8":
                    if (metaField.getKey().equals("COUNT(*)")) {
                        createSQL += "count bigint, ";
                    } else if (metaField.getKey().matches("^SUM.*")) {
                        createSQL += "double bigint, ";
                    } else {
                        createSQL += metaField.getKey() + " bigint, ";
                    }
                    break;
                case "varchar":
                    createSQL += metaField.getKey() + " varchar(255), ";
                    break;
                case "float":
                case "numeric":
                    createSQL += metaField.getKey() + " float, ";
                    break;
                case "decimal":
                    createSQL += metaField.getKey() + " decimal, ";
                    break;
                default:
                    createSQL += metaField.getKey() + " " + metaField.getValue().asText() + " ";
                    break;
            }

//            if (rsmd.getColumnName(i).equals("COUNT(*)")) {
//                sql2 += "count, ";
//            } else {
            insertSQL += metaField.getKey() + ", ";
//            }
        }

        createSQL = createSQL.substring(0, createSQL.length() - 2) + ")";

        insertSQL = insertSQL.substring(0, insertSQL.length() - 2);
        insertSQL += ") VALUES (";

        Log.out(createSQL);
        ps = con.prepareStatement(createSQL);
        ps.executeUpdate();

        // INSERT INTO
        for (JsonNode node : root.get("data")) {
            String tmpSQL = insertSQL;
            Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
            while (nodeFields.hasNext()) {
                Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                JsonNode jn = nodeField.getValue();
                if (jn.isTextual()) {
                    tmpSQL += "'" + nodeField.getValue().asText() + "', ";
                } else if (jn.isInt()) {
                    tmpSQL += nodeField.getValue().asInt() + ", ";
                } else if (jn.isDouble()) {
                    tmpSQL += nodeField.getValue().asDouble() + ", ";
                } else if (jn.isFloat()) {
                    tmpSQL += nodeField.getValue().asDouble() + ", ";
                } else if (nodeField.getValue().asText().equals("null")) {
                    tmpSQL += "null, ";
                }
            }
            tmpSQL = tmpSQL.substring(0, tmpSQL.length() - 2) + ")";
            //  System.out.println(tmpSQL);
            ps = con.prepareStatement(tmpSQL);
            ps.executeUpdate();
        }
    }

    // テーブル作成、INSERT処理まで行う
    public static void insertFromResultSet(String dbms, Connection con, ResultSet rs, String tmpdate, Boolean original) throws SQLException {

//        Connection con = null;
//        // psqlの場合は同一dbms内で別schemaに繋ぐため
//        switch (GlobalEnv.getDriver()) {
//            case "mysql":
//            case "sqlite":
//                con = DBConnect.connectLocalTmp();
//                break;
//            case "postgresql":
//                con = DBConnect.connectLocal();
//                break;
//        }

        PreparedStatement ps;

        ResultSetMetaData rsmd = rs.getMetaData();

        // CREATE, INSERTクエリ作成
        String sql = "CREATE TABLE ";
        String sql2 = "INSERT INTO ";

        switch (GlobalEnv.getDriver()) {
            case "mysql":
                if (original) {
                    sql += "result_" + tmpdate + " (";
                    sql2 += "result_" + tmpdate + " (";
                } else {
                    sql += rsmd.getTableName(1) + "_" + tmpdate + " (";
                    sql2 += rsmd.getTableName(1) + "_" + tmpdate + " (";
                }
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    // 取得内容確認
//                    System.out.printf("%s\t%s\t%s\t%d\t%d\t%d%n",
//                            rsmd.getColumnName(i),
//                            rsmd.getColumnTypeName(i),
//                            rsmd.getColumnClassName(i),
//                            rsmd.getColumnDisplaySize(i),
//                            rsmd.getPrecision(i),
//                            rsmd.getScale(i)
//                    );

                    switch (rsmd.getColumnClassName(i)) {
                        case "java.lang.Integer":
                            sql += rsmd.getColumnName(i) + " int, ";
                            break;
                        case "java.lang.Double":
                            if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum double, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " double, ";
                            }
                        case "java.lang.Long":
                            if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                                sql += "count bigint, ";
                            } else {
//						sql += rsmd.getColumnName(i) + " bigint, ";
                            }
                            break;
                        case "java.lang.String":
                            sql += rsmd.getColumnName(i) + " varchar(255), ";
                            break;
                        case "java.lang.Float":
                            sql += rsmd.getColumnName(i) + " float, ";
                            break;
                        // とりあえず
                        case "java.math.BigDecimal":
                            if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum double, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " decimal, ";
                            }
                            break;
                        default:
                            break;
                    }

                    if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                        sql2 += "count, ";
                    } else if (rsmd.getColumnName(i).matches("^SUM.*")) {
                        sql2 += "sum, ";
                    } else {
                        sql2 += rsmd.getColumnName(i) + ", ";
                    }
                }
                sql = sql.substring(0, sql.length() - 2);
                sql += ")";

                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ") ";

                sql2 += "VALUES (";
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    sql2 += "?, ";
                }
                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ")";

                break;

            case "postgresql":
                if (original) {
                    sql += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
                    sql2 += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
                } else {
                    sql += GlobalEnv.getTmpdb() + "." + rsmd.getTableName(1) + "_" + tmpdate + " (";
                    sql2 += GlobalEnv.getTmpdb() + "." + rsmd.getTableName(1) + "_" + tmpdate + " (";
                }
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    // 取得内容確認
//                    System.out.printf("%s\t%s\t%s\t%d\t%d\t%d%n",
//                            rsmd.getColumnName(i),
//                            rsmd.getColumnTypeName(i),
//                            rsmd.getColumnClassName(i),
//                            rsmd.getColumnDisplaySize(i),
//                            rsmd.getPrecision(i),
//                            rsmd.getScale(i)
//                    );

                    switch (rsmd.getColumnClassName(i)) {
                        case "java.lang.Integer":
                            sql += rsmd.getColumnName(i) + " int, ";
                            break;
                        case "java.lang.Long":
                            if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                                sql += "count bigint, ";
                            } else if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum double, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " bigint, ";
                            }
                            break;
                        case "java.lang.String":
                            sql += rsmd.getColumnName(i) + " varchar, ";
                            break;
                        case "java.lang.Float":
                            sql += rsmd.getColumnName(i) + " float, ";
                            break;
                        // とりあえず
                        case "java.math.BigDecimal":
                            sql += rsmd.getColumnName(i) + " decimal, ";
                            break;
                        default:
                            break;
                    }

                    if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                        sql2 += "count, ";
                    } else if (rsmd.getColumnName(i).matches("^SUM.*")) {
                        sql2 += "sum, ";
                    } else {
                        sql2 += rsmd.getColumnName(i) + ", ";
                    }
                }
                sql = sql.substring(0, sql.length() - 2);
                sql += ")";

                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ") ";

                sql2 += "VALUES (";
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    sql2 += "?, ";
                }
                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ")";

                break;

            case "sqlite":
                if (original) {
                    sql += "result_" + tmpdate + " (";
                    sql2 += "result_" + tmpdate + " (";
                } else {
                    sql += rsmd.getTableName(1) + "_" + tmpdate + " (";
                    sql2 += rsmd.getTableName(1) + "_" + tmpdate + " (";
                }
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    // 取得内容確認
//                    System.out.printf("%s\t%s\t%s\t%d\t%d\t%d%n",
//                            rsmd.getColumnName(i),
//                            rsmd.getColumnTypeName(i),
//                            rsmd.getColumnClassName(i),
//                            rsmd.getColumnDisplaySize(i),
//                            rsmd.getPrecision(i),
//                            rsmd.getScale(i)
//                    );

                    switch (rsmd.getColumnTypeName(i).toUpperCase()) {
                        case "INTEGER":
                        case "INT":
                        case "INT4":
                        case "INT8":
                        case "SERIAL":
                            if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum int, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " int, ";
                            }
                            break;
                        case "DOUBLE":
                            if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum double, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " double, ";
                            }
                        case "LONG":
                            if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                                sql += "count bigint, ";
                            } else {
                                 sql += rsmd.getColumnName(i) + " bigint, ";
                            }
                            break;
                        case "STRING":
                        case "VARCHAR":
                            sql += rsmd.getColumnName(i) + " varchar, ";
                            break;
                        case "FLOAT":
                            sql += rsmd.getColumnName(i) + " float, ";
                            break;
                        // とりあえず
                        case "BIGDECIMAL":
                        case "DECIMAL":
                        case "NUMERIC":
                            if (rsmd.getColumnName(i).matches("^SUM.*")) {
                                sql += "sum double, ";
                            } else {
                                sql += rsmd.getColumnName(i) + " decimal, ";
                            }
                            break;
                        default:
                            break;
                    }

                    if (rsmd.getColumnName(i).equals("COUNT(*)")) {
                        sql2 += "count, ";
                    } else if (rsmd.getColumnName(i).matches("^SUM.*")) {
                        sql2 += "sum, ";
                    } else {
                        sql2 += rsmd.getColumnName(i) + ", ";
                    }
                }
                sql = sql.substring(0, sql.length() - 2);
                sql += ")";

                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ") ";

                sql2 += "VALUES (";
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    sql2 += "?, ";
                }
                sql2 = sql2.substring(0, sql2.length() - 2);
                sql2 += ")";

                break;

            default:
                System.out.println("Sorry, " + dbms + " is not supported.");
                break;
        }

        Log.out(sql);
        Log.out(sql2 + "\n");

        ps = con.prepareStatement(sql);
        ps.executeUpdate();

        ps = con.prepareStatement(sql2);

        while (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                switch (rsmd.getColumnTypeName(i).toUpperCase()) {
                    case "INTEGER":
                    case "INT":
                    case "INT4":
                    case "SERIAL":
                        ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "DOUBLE":
                        ps.setDouble(i, rs.getDouble(rsmd.getColumnName(i)));
                        break;
                    case "LONG":
                        ps.setLong(i, rs.getLong(rsmd.getColumnName(i)));
                        break;
                    case "STRING":
                    case "VARCHAR":
                        ps.setString(i, rs.getString(rsmd.getColumnName(i)));
                        break;
                    case "FLOAT":
                        ps.setFloat(i, rs.getFloat(rsmd.getColumnName(i)));
                        break;
                    case "BIGDECIMAL":
                    case "DECIMAL":
                    case "NUMERIC":
                        ps.setBigDecimal(i, rs.getBigDecimal(rsmd.getColumnName(i)));
                        break;
                    default:
                        System.out.println("defaulted on:" + rsmd.getColumnClassName(i));
                        break;
                }
            }
            ps.executeUpdate();
        }

        DBConnect.close(con);
    }

}
