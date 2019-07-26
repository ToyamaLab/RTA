package rtaserver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.core.QueryExecutor;
import rtaserver.common.*;
import rtaserver.db.DBConnect;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.*;



@Path("/")
public class doSemiJoin {


    @POST
    @Path("/doSemiJoin")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String sample(String  rs_json) throws ClassNotFoundException, SQLException {

        System.out.println("begining SJ");
        System.out.println(rs_json+"\n");

        GlobalEnv.setGlobalEnv();

        Connection conn = null;

        switch (GlobalEnv.getDriver()){
            case "mysql":
                Class.forName("com.mysql.jdbc.Driver");
                conn = DBConnect.connectLocalTmp();
                break;
            case "postgresql":
                Class.forName("org.postgresql.Driver");
                conn = DBConnect.connectLocal();
                break;

            //did not implement the case sqlite
            case "sqlite":
                conn = DBConnect.connectLocalTmp();
                Statement s = conn.createStatement();
                s.execute("ATTACH \"" + GlobalEnv.getDb() + "\" as local");

            default:
                break;
        }

        // JSONの解析
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(rs_json);
        } catch (IOException e) {
            e.printStackTrace();
        }


        String localSJTable=root.get("on_site_table_name").asText();                                      // NOT DONE HERE

        List<String> importedSJAttributs= new ArrayList<>();
        String newTableName= localSJTable + "_up";

        JsonNode dataSJ = null;
        String importedSJTable="";

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
        String tmpdate = sdf.format(date);


        switch (root.get("accessMethode").asText()){
            case "on_site":
                importedSJTable=root.get("not_on_site_table_name").asText();
                for (JsonNode node : root.get("not_on_site_join_attributs")) {
                    Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
                    while (nodeFields.hasNext()) {
                        Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                        importedSJAttributs.add(nodeField.getValue().asText());
                    }
                }
                break;

            case "local": // it is 'local' for the client so from the server site, it is a remote db
                try {

                    dataSJ = mapper.readTree(rs_json);
                    importedSJTable = createImportedTable.create(dataSJ,importedSJAttributs,conn,tmpdate);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                break;

            case "API":
                try {
                    String apiDataJSON = fetchSemiJoinColumnsAPI.fetch(root.get("host").asText(),rs_json);
                    dataSJ = mapper.readTree(apiDataJSON);
                    importedSJTable = createImportedTable.create(dataSJ,importedSJAttributs,conn,tmpdate);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;

            case "direct":

                importedSJTable= fetchCreateSemiJoinColumnsDirectly.fetch(importedSJAttributs,root.get("connector").asText(),root.get("user").asText(),root.get("password").asText(),root.get("dbms").asText(),root.get("sqlQuery").asText(), tmpdate);
                break;
        }


        String createSQL="CREATE TABLE "+newTableName+" AS SELECT * FROM "+localSJTable +" WHERE EXISTS ( SELECT 1 FROM " + importedSJTable + " WHERE ";
        String statSQL = "SELECT 'table' AS \"set\",COUNT(*) FROM "+newTableName;

        Iterator<String> importedSJAttribut = importedSJAttributs.iterator();
        for (JsonNode node : root.get("on_site_join_attributs")) {
            Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
            while (nodeFields.hasNext()) {
                Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                createSQL+= localSJTable+"."+nodeField.getValue().asText()+"="+importedSJTable+"."+importedSJAttribut.next()+" AND ";

            }
        }
        for (JsonNode node : root.get("on_site_all_attributs")) {
            Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
            while (nodeFields.hasNext()) {
                Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                statSQL+= " UNION SELECT '"+nodeField.getValue().asText()+"' AS \"set\",COUNT(DISTINCT "+nodeField.getValue().asText()+") FROM "+newTableName;

            }
        }

        createSQL = createSQL.substring(0, createSQL.length() - 4) + ")";

        System.out.println("doSJ : "+ createSQL);


        String return_data="{\"table\":\""+newTableName+"\",";
        String return_att_cardi = "";
        PreparedStatement ps2 = conn.prepareStatement(createSQL);
        ps2.executeUpdate();



        try {
            PreparedStatement ps = conn.prepareStatement(statSQL);
            ResultSet rs = ps.executeQuery();

            while(rs.next()) {
                switch (rs.getString(1)) {
                    case "table":
                        return_data += "\"cardinality_tbl\":" + rs.getInt(2) + ",";
                        break;
                    default:
                        return_att_cardi += "{\"att\":\"" + rs.getString(1) + "\",\"cardi\":" + rs.getInt(2) + "},";
                }
            }
            return_data += "\"cardinality_att\":[" + return_att_cardi.substring(0, return_att_cardi.length() - 1) + "]}";
        } catch (SQLException e) {
            e.printStackTrace();
            return "error_in_query : " + e.toString();
        }

        System.out.println("fin doSJ "+return_data);

    return return_data;
    }
}