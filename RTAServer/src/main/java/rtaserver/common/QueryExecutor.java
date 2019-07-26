package rtaserver.common;

import java.sql.*;
import java.util.List;

import rtaserver.db.DBConnect;


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


    // テーブル作成、INSERT処理まで行う
    public static String insertFromResultSet(List<String> importedSJAttribut, String dbms, Connection con, ResultSet rs, String tmpdate) throws SQLException {

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
        String importedSJTable="";

        ResultSetMetaData rsmd = rs.getMetaData();

        // CREATE, INSERTクエリ作成
        String sql = "CREATE TABLE ";
        String sql2 = "INSERT INTO ";


        switch (GlobalEnv.getDriver()) {
            case "mysql":
                importedSJTable="sjimported_"+rsmd.getTableName(1) + "_" + tmpdate;
                sql +=  importedSJTable+ " (";
                sql2 += importedSJTable+ " (";
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

                    importedSJAttribut.add(rsmd.getColumnName(i));

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
                importedSJTable = GlobalEnv.getTmpdb() + ".sjimported_" + rsmd.getTableName(1) + "_" + tmpdate;
                sql +=  importedSJTable + " (";
                sql2 += importedSJTable + " (";
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

                    importedSJAttribut.add(rsmd.getColumnName(i));

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
                        case "java.lang.Double":
                            sql += rsmd.getColumnName(i) + " float8, ";
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
                importedSJTable ="sjimported_"+rsmd.getTableName(1) + "_" + tmpdate;
                sql += importedSJTable + " (";
                sql2 += importedSJTable + " (";
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

                    importedSJAttribut.add(rsmd.getColumnName(i));

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
                    case "FLOAT8":
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
        return importedSJTable;

    }
}
