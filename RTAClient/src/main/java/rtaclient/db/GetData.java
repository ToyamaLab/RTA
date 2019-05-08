package rtaclient.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;

public class GetData {
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
    
    public static ResultSet fromSparql(String queryString,String endpoint){
   	 
   	 Query query = QueryFactory.create(queryString);
			QueryExecution qexec = QueryExecutionFactory
					.sparqlService(endpoint, query);
			ResultSet rs = qexec.execSelect();
   	 return rs;
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
}
