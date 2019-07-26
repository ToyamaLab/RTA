package rtaclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.statement.select.SelectItem;
import rtaclient.common.GlobalEnv;
import rtaclient.db.DBConnect;
import rtaclient.parser.Parser;
import rtaclient.sjmanager.attributDAGRoot;
import rtaclient.sjmanager.tableDAGRoot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class InitialExecution {
    public static int getLocalStat(TableConnector tc, Parser parser) throws SQLException {
        java.util.Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
        String tmpdate = sdf.format(date);

        Connection con_local=null;
        String tmp_table = "";
        String sqlQuery = "CREATE TABLE ";
        switch (GlobalEnv.getDriver()) {
            case "mysql":
                con_local = DBConnect.connectLocalTmp();
                tmp_table = tc.getTbName() + "_" + tmpdate;
                sqlQuery += tmp_table + " AS ";
                break;
            case "postgresql":
                con_local = DBConnect.connectLocal();
                tmp_table = GlobalEnv.getTmpdb() + "."+ tc.getTbName() + "_" + tmpdate;
                sqlQuery += tmp_table + " AS ";
                break;
            case "sqlite":
                con_local = DBConnect.connectLocalTmp();
                Statement s = con_local.createStatement();
                s.execute("ATTACH \"" + GlobalEnv.getDb() + "\" as local");
                tmp_table = GlobalEnv.getTmpdb() + "."+ tc.getTbName() + "_" + tmpdate;
                sqlQuery += tmp_table + " AS ";
        }

        sqlQuery+= tc.getSQL();

        PreparedStatement ps_local = con_local.prepareStatement(sqlQuery);
        ps_local.executeUpdate();


        sqlQuery = "SELECT 'table_init' AS \"table\",'table' AS \"set\", COUNT(*) FROM "+tc.getTbName()+" UNION SELECT 'table_after_select' AS table, 'table' AS \"set\",COUNT(*) FROM "+tmp_table;
        for (SelectItem si : tc.getSelectItem()){
            sqlQuery += " UNION SELECT 'table_init' AS \"table\",'"+selectName(si.toString())+"' AS \"set\",COUNT(DISTINCT "+selectName(si.toString())+") FROM "+tc.getTbName();
            sqlQuery += " UNION SELECT 'table_after_select' AS \"table\",'"+selectName(si.toString())+"' AS \"set\",COUNT (DISTINCT " +selectName(si.toString())+") FROM "+tmp_table;
        }

        ps_local = con_local.prepareStatement(sqlQuery);
        ResultSet rs_local = ps_local.executeQuery();


        tableDAGRoot root = new tableDAGRoot(tc.getTbName());
        parser.getTableDAGRoots().put(tc.getTbName(),root);
        root.addSelectNodeToTop();


        while(rs_local.next()) {
            switch (rs_local.getString(1)) {
                case "table_init":
                    switch (rs_local.getString(2)){
                        case "table":
                            root.getFirstNode().setTableCardinality(rs_local.getInt(3));
                            break;
                        default:
                            root.getFirstNode().putAttributCardinality(rs_local.getString(2),rs_local.getInt(3));
                            attributDAGRoot ar = new attributDAGRoot(rs_local.getString(2),rs_local.getInt(3));
                            tc.getAttributDAGRoots().put(rs_local.getString(2),ar);
                    }
                    break;
                case "table_after_select":
                    switch (rs_local.getString(2)){
                        case "table":
                            root.getTopNode().setTableCardinality(rs_local.getInt(3));
                            break;
                        default:
                            root.getTopNode().putAttributCardinality(rs_local.getString(2),rs_local.getInt(3));
                    }
            }
        }

        for (Map.Entry current : root.getTopNode().getAttributCardinality().entrySet()){
            tc.getAttributDAGRoots().get(current.getKey()).addSelectNodeToTop((int) current.getValue());
        }


        return 0;
    }


    public static int getDirectStat(TableConnector tc, Parser parser) throws SQLException {
        Connection con_remote;
        con_remote = DBConnect.connect(tc.createConnector(), tc.getUser(), tc.getPassword());

        String sqlQuery = "SELECT 'table_init' AS \"table\",'table' AS \"set\",COUNT(*) FROM "+tc.getTbName()+" UNION SELECT 'table_after_select' AS table, 'table' AS \"set\",COUNT(*) FROM ("+tc.getSQL()+") AS tbl";
        for (SelectItem si : tc.getSelectItem()){
            sqlQuery += " UNION SELECT 'table_init' AS \"table\",'"+selectName(si.toString())+"' AS \"set\",COUNT(DISTINCT "+selectName(si.toString())+") FROM "+tc.getTbName();
            sqlQuery += " UNION SELECT 'table_after_select' AS \"table\",'"+selectName(si.toString())+"' AS \"set\",COUNT(DISTINCT "+selectName(si.toString())+") FROM ("+tc.getSQL()+") AS tbl";
        }


        PreparedStatement ps_remote = con_remote.prepareStatement(sqlQuery);
        ResultSet rs_remote=ps_remote.executeQuery();

        tableDAGRoot root = new tableDAGRoot(tc.getTbName());
        parser.getTableDAGRoots().put(tc.getTbName(),root);
        root.addSelectNodeToTop();


        while(rs_remote.next()) {
            switch (rs_remote.getString(1)) {
                case "table_init":
                    switch (rs_remote.getString(2)){
                        case "table":
                            root.getFirstNode().setTableCardinality(rs_remote.getInt(3));
                            break;
                        default:
                            root.getFirstNode().putAttributCardinality(rs_remote.getString(2),rs_remote.getInt(3));
                            attributDAGRoot ar = new attributDAGRoot(rs_remote.getString(2),rs_remote.getInt(3));
                            tc.getAttributDAGRoots().put(rs_remote.getString(2),ar);
                    }
                    break;
                case "table_after_select":
                    switch (rs_remote.getString(2)){
                        case "table":
                            root.getTopNode().setTableCardinality(rs_remote.getInt(3));
                            break;
                        default:
                            root.getTopNode().putAttributCardinality(rs_remote.getString(2),rs_remote.getInt(3));
                    }
            }
        }

        for (Map.Entry current : root.getTopNode().getAttributCardinality().entrySet()){
            tc.getAttributDAGRoots().get(current.getKey()).addSelectNodeToTop((int) current.getValue());
        }

        return 0;
    }





    public static String executeAPIinit(TableConnector tc, Parser parser) {
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        // TODO: 実際のリクエスト先を指定
        String url = tc.getHost() + "/execInitQuery";


        String post_data = "{\"table\":\""+ tc.getAccessName() + "\",\"query\":\"" + tc.getSQL()+"\",\"table_attributs\":[";

        for( SelectItem si : tc.getSelectItem()){
            post_data += "{\"att\":\""+si+"\"},";
        }


        post_data= post_data.substring(0,post_data.length()-1)+"]}";

        tableDAGRoot root = new tableDAGRoot(tc.getTbName());
        parser.getTableDAGRoots().put(tc.getTbName(),root);
        root.addSelectNodeToTop();


        HttpURLConnection httpURLConnection = null;
        try {

            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setRequestProperty("Content-Type", "text/plain; utf-8");
            httpURLConnection.setRequestProperty("Accept", "text/plain");
            try(OutputStream osOutputStream = httpURLConnection.getOutputStream()) {
                osOutputStream.write(post_data.getBytes(), 0, post_data.getBytes().length);
            }


            bufferedReader = new BufferedReader(
                    new InputStreamReader(
                            httpURLConnection.getInputStream(), "UTF-8"));

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }

//			System.out.println(stringBuilder.toString());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonRoot = null;
            try {
                jsonRoot = mapper.readTree(stringBuilder.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            root.getFirstNode().setTableCardinality(jsonRoot.get("cardinality_tbl_init").asInt());
            root.getTopNode().setTableCardinality(jsonRoot.get("cardinality_tbl_after_select").asInt());

            for (JsonNode node : jsonRoot.get("cardinality_att_init")) {
                Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
                String att = "";
                int cardi=0;
                while (nodeFields.hasNext()) {
                    Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                    JsonNode jn = nodeField.getValue();
                    if (jn.isTextual()) {
                        att=nodeField.getValue().asText();
                    } else if (jn.isInt()) {
                        cardi=nodeField.getValue().asInt();
                    }
                }
                root.getFirstNode().putAttributCardinality(att,cardi);
                attributDAGRoot ar = new attributDAGRoot(att,cardi);
                tc.getAttributDAGRoots().put(att,ar);
            }

            for (JsonNode node : jsonRoot.get("cardinality_att_after_select")) {
                Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
                String att = "";
                int cardi=0;
                while (nodeFields.hasNext()) {
                    Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                    JsonNode jn = nodeField.getValue();
                    if (jn.isTextual()) {
                        att=nodeField.getValue().asText();
                    } else if (jn.isInt()) {
                        cardi=nodeField.getValue().asInt();
                    }
                }
                root.getTopNode().putAttributCardinality(att,cardi);
            }

            for (Map.Entry current : root.getTopNode().getAttributCardinality().entrySet()){
                tc.getAttributDAGRoots().get(current.getKey()).addSelectNodeToTop((int) current.getValue());
            }

            return jsonRoot.get("table").asText();

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

    public static String selectName(String s){
        for (int i=0; i<s.length();++i){
            if (s.charAt(i)=='.'){
                return s.substring(i+1);
            }
        }
        return s;
    }
}
