package rtaclient.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.URLConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class GetData {
    public static String result( String url,String tableName) {
        // TODO: accessNameを無くてもテーブル名が取得できるようにkosakaに変えてもらう
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        // TODO: 実際のリクエスト先を指定
        url += "/retrieveResult";
        url += "?table="+tableName;


        HttpURLConnection httpURLConnection = null;
        try {

            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoInput(true);
            httpURLConnection.setRequestMethod("GET");



            bufferedReader = new BufferedReader(
                    new InputStreamReader(
                            httpURLConnection.getInputStream(), "UTF-8"));

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
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

    public static String columnSJ( String url, String table, List<String> attributSJ) {
        // TODO: accessNameを無くてもテーブル名が取得できるようにkosakaに変えてもらう
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        // TODO: 実際のリクエスト先を指定
        url += "/fetchSemiJoinColumn";


        for (String e : attributSJ){
            table+=","+e;
        }

        HttpURLConnection httpURLConnection = null;
        try {

            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setRequestProperty("Content-Type", "text/plain; utf-8");
            httpURLConnection.setRequestProperty("Accept", "text/plain");
            try(OutputStream osOutputStream = httpURLConnection.getOutputStream()) {
                osOutputStream.write(table.getBytes(), 0, table.getBytes().length);
            }


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
}
