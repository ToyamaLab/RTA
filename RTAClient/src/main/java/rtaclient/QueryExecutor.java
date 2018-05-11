package rtaclient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import rtaclient.common.GlobalEnv;
import rtaclient.common.Log;
import rtaclient.db.DBConnect;

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

    public static void insertFromJson(ArrayList<TableConnector> tcs, String rs_json, String tmpdate, Boolean original) throws SQLException {

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

        List<String> tableNames = new ArrayList<>();
        String tableName="";
        for(int i=0;i<tcs.size();i++){
			TableConnector tc = tcs.get(i);
	    	for(int j=0;j<tc.getTables().size();j++){
	    		if(j==0){
	    			tableName = tc.getTables().get(j);
	    		}else{
	    			tableName = tableName+"_"+tc.getTables().get(j);
	    		}
	    	}
	    	tableNames.add(tableName);
    	}

        PreparedStatement ps;
        long start,end;
        System.out.println("0");
        start = System.currentTimeMillis();
      //JsonObject {}
	    JsonObject jsonObj = (JsonObject) new Gson().fromJson(rs_json, JsonObject.class);
	    end = System.currentTimeMillis();
	    System.out.println("check1:"+(end-start));
	    //JsonArray []
	    JsonArray results = jsonObj.get("Results").getAsJsonArray();
	    end = System.currentTimeMillis();
	    System.out.println("check2:"+(end-start));
	    for(int i=0; i<results.size(); i++){
	    	JsonObject result = results.get(i).getAsJsonObject();

	        // SQL前半部
	        String createSQL = "CREATE TABLE ";
	        String insertSQL = "INSERT INTO ";
	        switch (GlobalEnv.getDriver()) {
	            case "mysql":
	                if (original) {
	                    createSQL += "result_" + tmpdate + " (";
	                    insertSQL += "result_" + tmpdate + " (";
	                } else {
	                    createSQL += tableNames.get(i) + "_" + tmpdate + " (";
	                    insertSQL += tableNames.get(i) + "_" + tmpdate + " (";
	                }

	                break;
	            case "postgresql":
	            case "sqlite":
	                if (original) {
	                    createSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
	                    insertSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
	                } else {
	                    createSQL += GlobalEnv.getTmpdb() + "." + tableNames.get(i) + "_" + tmpdate + " (";
	                    insertSQL += GlobalEnv.getTmpdb() + "." + tableNames.get(i) + "_" + tmpdate + " (";
	                }

	                break;

	            default:
	                break;
		    }

	        JsonObject metadata = result.get("metadata").getAsJsonObject();

	        JsonArray dataArray = result.get("result").getAsJsonArray();

	        ArrayList<String> columnTypes = new ArrayList<>();

	        Iterator<Entry<String, JsonElement>> metadataSet = metadata.entrySet().iterator();

    	    for(int m=0;m<metadata.size();m++){
    	    	Entry<String, JsonElement> md = metadataSet.next();
    	    	columnTypes.add(md.getValue().getAsString());
    	    	 System.out.println("ADD:"+md.getValue().getAsString());
    	    	switch (columnTypes.get(m)) {
        		case "int":
        		case "int4":
        			createSQL += md.getKey() + " int, ";
        			break;
        		case "bigint":
        		case "int8":
        			if (md.getKey().equals("COUNT(*)")) {
        				createSQL += "count bigint, ";
        			} else if (md.getKey().matches("^SUM.*")) {
        				createSQL += "double bigint, ";
        			} else {
        				createSQL += md.getKey() + " bigint, ";
        			}
        			break;
        		case "varchar":
        			createSQL += md.getKey() + " varchar(255), ";
        			break;
        		case "float":
        		case "numeric":
        			createSQL += md.getKey() + " float, ";
        			break;
        		case "decimal":
        			createSQL += md.getKey() + " decimal, ";
        			break;
        		default:
        			createSQL += md.getKey() + " " + md.getValue().getAsString() + ", ";
        			break;
	            }
        		insertSQL += md.getKey() + ", ";
        	}

    	    createSQL = createSQL.substring(0, createSQL.length() - 2) + ")";
    	    ps = con.prepareStatement(createSQL);
    	    ps.executeUpdate();
    	    insertSQL = insertSQL.substring(0, insertSQL.length() - 2);
    	    insertSQL += ") VALUES ";
    	    end = System.currentTimeMillis();
    	    System.out.println("check2.5:"+(end-start));
    	    int csize = columnTypes.size();
    	    String tuples="";
    	    for(int j=0;j<dataArray.size();j++){
    	    	JsonArray data = dataArray.get(j).getAsJsonArray();
    	    	String tmp = "(";
    	    	String tmp2 = insertSQL;
    	    	for(int k=0;k<csize;k++){
    	    		switch (columnTypes.get(k)) {
            		case "int":
            			tmp += data.get(k).getAsInt() + ",";
            			break;
            		case "int4":
            			tmp += data.get(k).getAsInt() + ",";
            			break;
            		case "bigint":
            			tmp += data.get(k).getAsBigInteger() + ",";
            			break;
            		case "int8":
            			tmp += data.get(k).getAsInt() + ",";
            			break;
            		case "varchar":
            			tmp += "'" + data.get(k).getAsString() + "',";
            			break;
            		case "float":
            			tmp += data.get(k).getAsFloat() + ",";
            			break;
            		case "numeric":
            			tmp += data.get(k).getAsFloat() + ",";
            			break;
            		case "decimal":
            			tmp += data.get(k).getAsFloat() + ",";
            			break;
            		default:
            			tmp += "'" + data.get(k).getAsString() + "',";
            			break;
    	            }
    	    	}
    	    	tmp = tmp.substring(0, tmp.length()-1)+")";
    	    	tmp2 += tmp;
    	    	ps = con.prepareStatement(tmp2);
        	    ps.executeUpdate();
    	    }
//    	    System.out.println("create:"+createSQL);
//    	    System.out.println("insert:"+insertSQL);
    	    System.out.println("SIZEEEE:"+dataArray.size());
          }
	    end = System.currentTimeMillis();
	    System.out.println("check3:"+(end-start));
    }

    public static void insertFromJson2(ArrayList<TableConnector> tcs, String rs_json, String tmpdate, Boolean original) throws SQLException {

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

        List<String> tableNames = new ArrayList<>();
        String tableName="";
        for(int i=0;i<tcs.size();i++){
			TableConnector tc = tcs.get(i);
	    	for(int j=0;j<tc.getTables().size();j++){
	    		if(j==0){
	    			tableName = tc.getTables().get(j);
	    		}else{
	    			tableName = tableName+"_"+tc.getTables().get(j);
	    		}
	    	}
	    	tableNames.add(tableName);
    	}

        PreparedStatement ps;

        String temp = rs_json.replaceAll("query_id", "");
        int size = rs_json.length() - temp.length() / "query_id".length();

        String tmp = rs_json.replaceAll("\"", "");
        tmp = tmp.replaceAll("{Results:", "");
        tmp = tmp.substring(0, tmp.length()-1);

        String tmp1 = rs_json.replaceAll("\"", "");
        String[] tmp2 = rs_json.split("metadata:");
        String[] tmp3 = tmp2[1].split(",");

      //JsonObject {}
	    JsonObject jsonObj = (JsonObject) new Gson().fromJson(rs_json, JsonObject.class);

	    //JsonArray []
	    JsonArray results = jsonObj.get("Results").getAsJsonArray();

	    for(int i=0; i<results.size(); i++){
	    	JsonObject result = results.get(i).getAsJsonObject();

	        // SQL前半部
	        String createSQL = "CREATE TABLE ";
	        String insertSQL = "INSERT INTO ";
	        switch (GlobalEnv.getDriver()) {
	            case "mysql":
	                if (original) {
	                    createSQL += "result_" + tmpdate + " (";
	                    insertSQL += "result_" + tmpdate + " (";
	                } else {
	                    createSQL += tableNames.get(i) + "_" + tmpdate + " (";
	                    insertSQL += tableNames.get(i) + "_" + tmpdate + " (";
	                }

	                break;
	            case "postgresql":
	            case "sqlite":
	                if (original) {
	                    createSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
	                    insertSQL += GlobalEnv.getTmpdb() + ".result_" + tmpdate + " (";
	                } else {
	                    createSQL += GlobalEnv.getTmpdb() + "." + tableNames.get(i) + "_" + tmpdate + " (";
	                    insertSQL += GlobalEnv.getTmpdb() + "." + tableNames.get(i) + "_" + tmpdate + " (";
	                }

	                break;

	            default:
	                break;
		    }

	        JsonObject metadata = result.get("metadata").getAsJsonObject();

	        JsonArray dataArray = result.get("result").getAsJsonArray();

	        ArrayList<String> columnTypes = new ArrayList<>();

	        Iterator<Entry<String, JsonElement>> metadataSet = metadata.entrySet().iterator();

    	    for(int m=0;m<metadata.size();m++){
    	    	Entry<String, JsonElement> md = metadataSet.next();
    	    	columnTypes.add(md.getValue().getAsString());
    	    	 System.out.println("ADD:"+md.getValue().getAsString());
    	    	switch (columnTypes.get(m)) {
        		case "int":
        		case "int4":
        			createSQL += md.getKey() + " int, ";
        			break;
        		case "bigint":
        		case "int8":
        			if (md.getKey().equals("COUNT(*)")) {
        				createSQL += "count bigint, ";
        			} else if (md.getKey().matches("^SUM.*")) {
        				createSQL += "double bigint, ";
        			} else {
        				createSQL += md.getKey() + " bigint, ";
        			}
        			break;
        		case "varchar":
        			createSQL += md.getKey() + " varchar(255), ";
        			break;
        		case "float":
        		case "numeric":
        			createSQL += md.getKey() + " float, ";
        			break;
        		case "decimal":
        			createSQL += md.getKey() + " decimal, ";
        			break;
        		default:
        			createSQL += md.getKey() + " " + md.getValue().getAsString() + ", ";
        			break;
	            }
        		insertSQL += md.getKey() + ", ";
        	}

    	    createSQL = createSQL.substring(0, createSQL.length() - 2) + ")";

    	    insertSQL = insertSQL.substring(0, insertSQL.length() - 2);
    	    insertSQL += ") VALUES (";

    	    for(int j=0;j<dataArray.size();j++){
    	    	JsonArray data = dataArray.get(j).getAsJsonArray();
    	    	for(int k=0;k<columnTypes.size();k++){
    	    		switch (columnTypes.get(k)) {
            		case "int":
            			insertSQL += data.get(k).getAsInt() + ",";
            			break;
            		case "int4":
            			insertSQL += data.get(k).getAsInt() + ",";
            			break;
            		case "bigint":
            			insertSQL += data.get(k).getAsBigInteger() + ",";
            			break;
            		case "int8":
            			insertSQL += data.get(k).getAsInt() + ",";
            			break;
            		case "varchar":
            			insertSQL += "'" + data.get(k).getAsString() + "',";
            			break;
            		case "float":
            			insertSQL += data.get(k).getAsFloat() + ",";
            			break;
            		case "numeric":
            			insertSQL += data.get(k).getAsFloat() + ",";
            			break;
            		case "decimal":
            			insertSQL += data.get(k).getAsFloat() + ",";
            			break;
            		default:
            			insertSQL += "'" + data.get(k).getAsString() + "',";
            			break;
    	            }
    	    	}
    	    	insertSQL = insertSQL.substring(0, insertSQL.length()-1) + "),(";
    	    }
    	    insertSQL = insertSQL.substring(0, insertSQL.length()-2);

    	    System.out.println("create:"+createSQL);
    	    System.out.println("insert:"+insertSQL);

    	    ps = con.prepareStatement(createSQL);
    	    ps.executeUpdate();

    	    ps = con.prepareStatement(insertSQL);
    	    ps.executeUpdate();

    	    Log.out(createSQL);
    	    Log.out(insertSQL);
          }
    }



    // テーブル作成、INSERT処理まで行う
    public static void insertFromResultSet(TableConnector tc, Connection con, ResultSet rs, String tmpdate, Boolean original) throws SQLException, ClassNotFoundException {

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

    	String dbms = tc.getDBMS();

    	String tableName="";
    	for(int i=0;i<tc.getTables().size();i++){
    		if(i==0){
    			tableName = tc.getTables().get(i);
    		}else{
    			tableName = tableName+"_"+tc.getTables().get(i);
    		}
    	}

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
                    sql += GlobalEnv.getTmpdb() + "." + tableName + "_" + tmpdate + " (";
                    sql2 += GlobalEnv.getTmpdb() + "." + tableName + "_" + tmpdate + " (";
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
                                // sql += rsmd.getColumnName(i) + " bigint, ";
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
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "INT2":
                        ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "INT4":
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
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
        System.out.println(sql);
        System.out.println(sql2);
        System.out.println(rsmd.getColumnCount());
        System.out.println("Insert Finish!!");
    }

    // テーブル作成、INSERT処理まで行う
    public static void insertFromResultSet(String dbms, Connection con, ResultSet rs, String tmpdate, Boolean original) throws SQLException, ClassNotFoundException {

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
                                // sql += rsmd.getColumnName(i) + " bigint, ";
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
        System.out.println(sql);
        System.out.println(sql2);

        ps = con.prepareStatement(sql);
        ps.executeUpdate();
        ps = con.prepareStatement(sql2);

        while (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                switch (rsmd.getColumnTypeName(i).toUpperCase()) {
                    case "INTEGER":
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "INT":
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "INT2":
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
                    case "INT4":
                    	ps.setInt(i, rs.getInt(rsmd.getColumnName(i)));
                        break;
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
                    	ps.setString(i, rs.getString(rsmd.getColumnName(i)));
                        break;
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
        System.out.println("Insert Finish!!");
    }
}
