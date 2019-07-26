package rtaserver.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class fetchSemiJoinColumnsAPI {

    public static String fetch(String url, String rs_json){

        System.out.println("fetchSJ");

        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        url+="/retrieveSemiJoinColumns";

        HttpURLConnection httpURLConnection = null;
        try {

            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setRequestProperty("Content-Type", "text/plain; utf-8");
            httpURLConnection.setRequestProperty("Accept", "text/plain");
            try(OutputStream osOutputStream = httpURLConnection.getOutputStream()) {
                osOutputStream.write(rs_json.getBytes(), 0, rs_json.getBytes().length);
            }


            bufferedReader = new BufferedReader(
                    new InputStreamReader(
                            httpURLConnection.getInputStream(), "UTF-8"));

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append("\r\n");
            }

//			System.out.println(stringBuilder.toString());
            System.out.println("fin fetchSJ");
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

}
