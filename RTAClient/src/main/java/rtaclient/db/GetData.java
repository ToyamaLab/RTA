package rtaclient.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import rtaclient.TableConnector;
import rtaclient.common.BloomFilter;
import rtaclient.parser.Parser;
import rtaclient.webService.Filter;
import rtaclient.webService.Query;

public class GetData {
	public static String fromSQL(ArrayList<TableConnector> tcs,Parser parser) throws JSQLParserException {
		BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        //String url = "131.113.101.113/rta/test3";
        //String url = "http://spacia.db.ics.keio.ac.jp:8080/rtaserver/jsonTest";
        //String url = "http://131.113.101.104:8080/rtaserver/jsonTest4";
        String url = "http://localhost:8080/rtaserver/jsonTest4";
        //String url = "http://:8080/rtaserver/jsonTest4";
        HttpURLConnection httpURLConnection = null;
        PlainSelect pl = (PlainSelect)parser.getOriginalSelect().getSelectBody();
    	String regex = "\\s(where|WHERE)\\s"; //str and ~的なパターン
    	Pattern p = Pattern.compile(regex);
    	Matcher m = p.matcher(pl.toString());
    	Expression expr = null;

        List<Table> froms = new ArrayList<>();
        Table t = new Table();
        t = (Table)pl.getFromItem();
        froms.add(t);
//	    for(int i=1;i<pl.getJoins().size()+1;i++){
//        t = (Table)pl.getJoins().get(i-1).getRightItem();
//        froms.add(t);
//    }
        System.out.println("pl:"+pl.toString());
        if(isJoins(pl)){
		    for(int i=1;i<pl.getJoins().size()+1;i++){
		        t = (Table)pl.getJoins().get(i-1).getRightItem();
		        froms.add(t);
		    }
        }

        List<Query> Queries =  new ArrayList<Query>();
        for(int i=0;i<tcs.size();i++){
//        	if(tcs.get(i).getFromItems().size()!=1){
        		PlainSelect pl2  = (PlainSelect)tcs.get(i).getSelect().getSelectBody();
        		List<SelectItem> sis = pl2.getSelectItems();
				for(int j = 0;j<sis.size();j++){
					SelectExpressionItem si = (SelectExpressionItem) sis.get(j);
					Column c = (Column) si.getExpression();
					for(int k=0;k<froms.size();k++){
						t  = (Table)froms.get(k);
						System.out.println(t.getAlias().getName());
						System.out.println(c.getTable().getName());
						if(t.getAlias().getName().equals(c.getTable().getName())){
							Alias alias = new Alias(t.getName()+"_"+c.getColumnName(),true);
							si.setAlias(alias);
						}
					}
//				}
        	}
        	Query query = new Query(i+1,tcs.get(i).getSelect().toString());
        	Queries.add(query);
        }
        if(m.find()){
        	expr = CCJSqlParserUtil.parseCondExpression(pl.getWhere().toString());
	        expr.accept(new ExpressionVisitorAdapter() {
	            @Override
	            protected void visitBinaryExpression(BinaryExpression expr) {
	                if (expr instanceof ComparisonOperator) {
	                	if(expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column){
		                	Column columnLeft = (Column) expr.getLeftExpression();
		                	Column columnRight = (Column) expr.getRightExpression();
		                	int joinCon1 = -1,joinCon2 = -1;
		                	for(int i=0;i<tcs.size();i++){
	                    		List<FromItem>  fi = tcs.get(i).getFromItems();
	                    		for(int j=0;j<fi.size();j++){
			                    	if(fi.get(j).getAlias().getName().equals(columnLeft.getTable().toString())){
			                    		joinCon1 = i;
			                    	};
			                    	if(fi.get(j).getAlias().getName().equals(columnRight.getTable().toString())){
			                    		joinCon2 = i;
			                    	};
	                    		}
	                    	}

		                	Connection conn = DBConnect.connectLocal();
		                	ResultSet rs_tmp = null;
		                    PreparedStatement ps_tmp = null;
		                	String sql = "select ";
		                	String table="",column="";
		                	double falsePositiveProbability = 0.1; //固定
	        				int expectedNumberOfElements = 5000; //可変
	        				BloomFilter bf = new BloomFilter(falsePositiveProbability, expectedNumberOfElements);
	        				//local - remote
		                	if(joinCon1 == -1 && joinCon2 != -1){
		                		System.out.println("test1");
		                		sql = sql + columnLeft.toString()+" from ";
		                		for(int j=0;j<froms.size();j++){
		                			System.out.println("--debug--");
		                			System.out.println("columnLeft.getTable().getName():"+columnLeft.getTable().getName());
		                			System.out.println("columnRight.getTable().getName():"+columnRight.getTable().getName());
		                			System.out.println("froms.get(j).getAlias().getName():"+froms.get(j).getAlias().getName());
		                			System.out.println("--debug--");
		                			if(columnLeft.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				System.out.println("test2");
		                				sql = sql + froms.get(j).toString();
		                			}
		                			if(columnRight.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				table = froms.get(j).getFullyQualifiedName();
		                				column = columnRight.getColumnName();
		                				System.out.println("test3");
		                				System.out.println(table+":"+column);

		                			}
		                		}
		                		try {
									ps_tmp = conn.prepareStatement(sql);
									rs_tmp = ps_tmp.executeQuery();
			        				while(rs_tmp.next()){
			        					bf.add(getValue(rs_tmp,1));
			        				}
								} catch (SQLException e) {
									// TODO 自動生成された catch ブロック
									e.printStackTrace();
								}
		                		System.out.println("TTTTTABLE:"+table+",CCCCCOLUMN:"+column);
		        				Filter f = new Filter(bf.getBitSet().toString(),table,column,expectedNumberOfElements);
		        				Query  query = Queries.get(joinCon2);
		        				query.addFilters(f);
		        			//remote - local
		                	}else if(joinCon1 != -1 && joinCon2 == -1){
		                		System.out.println("test4");
		                		sql = sql + columnRight.toString()+" from ";
		                		for(int j=0;j<froms.size();j++){
		                			if(columnRight.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				System.out.println("test5");
		                				sql = sql + froms.get(j).toString();
		                			}
		                			if(columnLeft.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				table = froms.get(j).getFullyQualifiedName();
		                				column = columnRight.getColumnName();
		                				System.out.println("test6");
		                				System.out.println(table+":"+column);
		                			}
		                		}
		                		try {
									ps_tmp = conn.prepareStatement(sql);
									rs_tmp = ps_tmp.executeQuery();
			        				while(rs_tmp.next()){
			        					bf.add(getValue(rs_tmp,1));
			        				}
								} catch (SQLException e) {
									// TODO 自動生成された catch ブロック
									e.printStackTrace();
								}
		                		System.out.println("TTTTTABLE:"+table+",CCCCCOLUMN:"+column);
		        				Filter f = new Filter(bf.getBitSet().toString(),table,column,expectedNumberOfElements);
		        				Query  query = Queries.get(joinCon1);
		        				query.addFilters(f);
		                	}
		                	System.out.println("sql:"+sql);
	                	}
	                }
	                super.visitBinaryExpression(expr);
	            }
	        });
        }
        String middleResult="";
//        String middleResult = toJson(Queries);
        for(int i=0;i<tcs.size();i++){
        	Gson gson = new Gson();
        	if(i==0){
        		middleResult = gson.toJson(Queries.get(i));
        	}else{
        		middleResult = middleResult+","+gson.toJson(Queries.get(i));
        	}
        }
        String finalResult = "{\"Queries\":[" + middleResult + "]}";
        System.out.println(finalResult);

        String json = finalResult;
        try {
			json = URLEncoder.encode(json, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
        System.out.println("URL:"+url);
        return callPost(url, json);
	}


	public static String fromSQL2(ArrayList<TableConnector> tcs,Parser parser) throws JSQLParserException {
		BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        //String url = "131.113.101.113/rta/test3";
        //String url = "http://spacia.db.ics.keio.ac.jp:8080/rtaserver/jsonTest";
        String url = "http://localhost:8080/rtaserver/jsonTest2";
        HttpURLConnection httpURLConnection = null;
        PlainSelect pl = (PlainSelect)parser.getOriginalSelect().getSelectBody();
    	String regex = "\\s(where|WHERE)\\s"; //str and ~的なパターン
    	Pattern p = Pattern.compile(regex);
    	Matcher m = p.matcher(pl.toString());
    	Expression expr = null;

        List<Table> froms = new ArrayList<>();
        Table t = new Table();
        t = (Table)pl.getFromItem();
        froms.add(t);
//	    for(int i=1;i<pl.getJoins().size()+1;i++){
//	        t = (Table)pl.getJoins().get(i-1).getRightItem();
//	        froms.add(t);
//	    }
        if(isJoins(pl)){
		    for(int i=1;i<pl.getJoins().size()+1;i++){
		        t = (Table)pl.getJoins().get(i-1).getRightItem();
		        froms.add(t);
		    }
        }

        List<Query> Queries =  new ArrayList<Query>();
        for(int i=0;i<tcs.size();i++){
//        	if(tcs.get(i).getFromItems().size()!=1){
        		PlainSelect pl2  = (PlainSelect)tcs.get(i).getSelect().getSelectBody();
        		List<SelectItem> sis = pl2.getSelectItems();
				for(int j = 0;j<sis.size();j++){
					SelectExpressionItem si = (SelectExpressionItem) sis.get(j);
					Column c = (Column) si.getExpression();
					for(int k=0;k<froms.size();k++){
						t  = (Table)froms.get(k);
						System.out.println(t.getAlias().getName());
						System.out.println(c.getTable().getName());
						if(t.getAlias().getName().equals(c.getTable().getName())){
							Alias alias = new Alias(t.getName()+"_"+c.getColumnName(),true);
							si.setAlias(alias);
						}
					}
//				}
        	}
        	Query query = new Query(i+1,tcs.get(i).getSelect().toString());
        	Queries.add(query);
        }
        if(m.find()){
        	expr = CCJSqlParserUtil.parseCondExpression(pl.getWhere().toString());
	        expr.accept(new ExpressionVisitorAdapter() {
	            @Override
	            protected void visitBinaryExpression(BinaryExpression expr) {
	                if (expr instanceof ComparisonOperator) {
	                	if(expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column){
		                	Column columnLeft = (Column) expr.getLeftExpression();
		                	Column columnRight = (Column) expr.getRightExpression();
		                	int joinCon1 = -1,joinCon2 = -1;
		                	for(int i=0;i<tcs.size();i++){
	                    		List<FromItem>  fi = tcs.get(i).getFromItems();
	                    		for(int j=0;j<fi.size();j++){
			                    	if(fi.get(j).getAlias().getName().equals(columnLeft.getTable().toString())){
			                    		joinCon1 = i;
			                    	};
			                    	if(fi.get(j).getAlias().getName().equals(columnRight.getTable().toString())){
			                    		joinCon2 = i;
			                    	};
	                    		}
	                    	}

		                	Connection conn = DBConnect.connectLocal();
		                	ResultSet rs_tmp = null;
		                    PreparedStatement ps_tmp = null;
		                	String sql = "select ";
		                	String table="",column="";
		                	double falsePositiveProbability = 0.1; //固定
	        				int expectedNumberOfElements = 200; //可変
	        				BloomFilter bf = new BloomFilter(falsePositiveProbability, expectedNumberOfElements);
	        				ArrayList<String> columns = new ArrayList<>();
	        				//local - remote
		                	if(joinCon1 == -1 && joinCon2 != -1){
		                		System.out.println("test1");
		                		sql = sql + columnLeft.toString()+" from ";
		                		for(int j=0;j<froms.size();j++){
		                			if(columnLeft.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				System.out.println("test2");
		                				sql = sql + froms.get(j).toString();
		                			}
		                			if(columnRight.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				table = froms.get(j).getFullyQualifiedName();
		                				column = columnRight.getColumnName();
		                				System.out.println("test3");
		                				System.out.println(table+":"+column);

		                			}
		                		}
		                		try {
									ps_tmp = conn.prepareStatement(sql);
									rs_tmp = ps_tmp.executeQuery();
			        				while(rs_tmp.next()){
			        					bf.add(getValue(rs_tmp,1));
			        					columns.add(getValue(rs_tmp,1));
			        				}
								} catch (SQLException e) {
									// TODO 自動生成された catch ブロック
									e.printStackTrace();
								}
		                		String tmp = "{"+columns.toString().substring(1, columns.toString().length()-1)+"}";
		        				System.out.println("TTTTTABLE:"+table+",CCCCCOLUMN:"+column);
		                		Filter f = new Filter(tmp,table,column,expectedNumberOfElements);
		        				Query  query = Queries.get(joinCon2);
		        				query.addFilters(f);
		        			//remote - local
		                	}else if(joinCon1 != -1 && joinCon2 == -1){
		                		System.out.println("test4");
		                		sql = sql + columnRight.toString()+" from ";
		                		for(int j=0;j<froms.size();j++){
		                			if(columnRight.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				System.out.println("test5");
		                				sql = sql + froms.get(j).toString();
		                			}
		                			if(columnLeft.getTable().getName().equals(froms.get(j).getAlias().getName())){
		                				table = froms.get(j).getFullyQualifiedName();
		                				column = columnRight.getColumnName();
		                				System.out.println("test6");
		                				System.out.println(table+":"+column);
		                			}
		                		}
		                		try {
									ps_tmp = conn.prepareStatement(sql);
									rs_tmp = ps_tmp.executeQuery();
			        				while(rs_tmp.next()){
			        					bf.add(getValue(rs_tmp,1));
			        					columns.add(getValue(rs_tmp,1));
			        				}
								} catch (SQLException e) {
									// TODO 自動生成された catch ブロック
									e.printStackTrace();
								}

		                		String tmp = "{"+columns.toString().substring(1, columns.toString().length()-1)+"}";
		        				Filter f = new Filter(tmp,table,column,expectedNumberOfElements);
		        				Query  query = Queries.get(joinCon1);
		        				query.addFilters(f);
		                	}
		                	System.out.println("sql:"+sql);
	                	}
	                }
	                super.visitBinaryExpression(expr);
	            }
	        });
        }
//        String middleResult="";

        String middleResult = toJson(Queries);

//        for(int i=0;i<tcs.size();i++){
//        	Gson gson = new Gson();
//        	if(i==0){
//        		middleResult = gson.toJson(Queries.get(i));
//        	}else{
//        		middleResult = middleResult+","+gson.toJson(Queries.get(i));
//        	}
//        }
        String finalResult = "{\"Queries\":[" + middleResult + "]}";
        System.out.println(finalResult);

        String json = finalResult;
        try {
			json = URLEncoder.encode(json, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
        System.out.println("URL:"+url);
        return callPost(url, json);
	}

    public static String fromSQL(String sql, String accessName) {
        // TODO: accessNameを無くてもテーブル名が取得できるようにkosakaに変えてもらう
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        // TODO: 実際のリクエスト先を指定
        String url = "http://131.113.101.113/rta/test3";
//      String encoded_sql = urlEncode(sql,"UTF-8");
//      url += encoded_sql;
//      url += "&table=" + accessName;
        HttpURLConnection httpURLConnection = null;
        try {
            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setDoInput(true);
            httpURLConnection.setRequestMethod("POST");
            OutputStream osOutputStream = httpURLConnection.getOutputStream();
            osOutputStream.write(("query=" + sql + "&table=" + accessName).getBytes());


            bufferedReader = new BufferedReader(
                    new InputStreamReader(
                            httpURLConnection.getInputStream(), "UTF-8"));

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\r\n");
            }

//			System.out.println(stringBuilder.toString());
            return stringBuilder.toString();

        } catch (MalformedURLException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
            return stringBuilder.toString();
        } catch (IOException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
            return stringBuilder.toString();
        }
    }

    public static String urlEncode(String str, String enc) {
        String urlEncode = "";
        StringBuffer result = new StringBuffer();

        try {
            urlEncode = URLEncoder.encode(str, enc);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.toString());
        }

        // 半角スペースは「%20」へ置換する
        for (char c : urlEncode.toCharArray()) {
            switch (c) {
                case '+':
                    result.append("%20");
                    break;
                default:
                    result.append(c);
                    break;
            }
        }

        return result.toString();
    }

	public static String getValue(ResultSet rs, int x) throws SQLException{
		ResultSetMetaData metadata = rs.getMetaData();
		String value = null;
		switch(metadata.getColumnType(x)){
	        case 12: //String
	                value = rs.getString(1);
	                break;
	        case 2: //numeric
	                value = rs.getString(1);
	                break;
	        case 4: //int
	                value = String.valueOf(rs.getInt(x));
	                break;
	        case -5: //bigint
	                value = String.valueOf(rs.getInt(x));
	                break;
	        case 91: //date
	                value = String.valueOf(rs.getDate(x));
	                break;
	        case 3: //DECIMAL
	                value = String.valueOf(rs.getInt(x));
	                break;
	        case 5: //SMALLINT
	                value = String.valueOf(rs.getInt(x));
	                break;
		}
		return value;
	}
	public static String callPost(String strPostUrl, String formParam){
		System.out.println("===== HTTP POST Start =====");
		String line = "";
		String result = "";
        try {
        	//URL url = new URL("http://spacia.db.ics.keio.ac.jp:8080/rtaserver/jsonTest");
        	//URL url = new URL("http://localhost:8080/rtaserver/jsonTest");
        	URL url = new URL(strPostUrl);
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setDoOutput(true);
                connection.setRequestMethod("POST");

                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(),StandardCharsets.UTF_8));
                writer.write(formParam);
                writer.flush();
                if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    try (InputStreamReader isr = new InputStreamReader(connection.getInputStream(),
                                                                       StandardCharsets.UTF_8);
                         BufferedReader reader = new BufferedReader(isr)) {

                        while ((line = reader.readLine()) != null) {
                        	if(result=="")
                        		result = line;
                        	else
                        		result = result + " " + line;
                        	System.out.println(line);
                        }
                    }
                }else if(connection.getResponseCode() == HttpURLConnection.HTTP_INTERNAL_ERROR){
                	System.out.println("内部エラーですーーーーーーー");
                }
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("===== HTTP POST End =====");
        return result;
	}

	static Boolean isJoins(PlainSelect pl){
		String q = pl.toString();
		String original = pl.toString();
    	String regex = "\\s(from|FROM)\\s{1,}\\S{1,}\\s{1,}\\S{1,}\\s{0,},"; //str.~的なパターン
    	Pattern p = Pattern.compile(regex);
    	Matcher m = p.matcher(q);
    	if(!m.matches())
    		return true;
    	else
    		return false;
	}

	static String toJson(List<Query> queries){
		String result = "";
		for(int i=0;i<queries.size();i++){
			String tmp = "";
			if(i!=0)
				result = result + ",";
			Query query = queries.get(i);
			tmp = "{\"ID\":"+query.getID()+",\"Query\":\""+query.getQuery()+"\",\"Filters\":[";
			for(int j=0;j<query.getFilters().size();j++){
				if(j==0)
					tmp = tmp+"{\"filter\":\""+query.getFilters().get(j).getFilter()+"\",\"table\":\""+query.getFilters().get(j).getTable()+"\",\"column\":\""+query.getFilters().get(j).getColumn()+"\",\"num_of_elements\":"+query.getFilters().get(j).getNum_of_elements()+"}";
				else
					tmp = tmp+",{\"filter\":\""+query.getFilters().get(j).getFilter()+"\",\"table\":\""+query.getFilters().get(j).getTable()+"\",\"column\":\""+query.getFilters().get(j).getColumn()+"\",\"num_of_elements\":"+query.getFilters().get(j).getNum_of_elements()+"}";
			}
			tmp = tmp+"]}";
			result = result + tmp;
		}
		return result;
	}

}
