package rtaserver.common;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

public class createImportedTable {

    public static String create(JsonNode dataSJ, List<String> importedSJAttributs, Connection conn, String tmpdate) throws SQLException {
        String importedTable="";
        switch (GlobalEnv.getDriver()){
            case "mysql":
                importedTable =dataSJ.get("not_on_site_table_name").asText()+"_imported";
                break;

            case "postgresql":
            case "sqlite":
                importedTable=GlobalEnv.getTmpdb() +"."+dataSJ.get("not_on_site_table_name").asText()+"_imported";
                break;
        }


        String createSQL = "CREATE TABLE "+importedTable+" (";
        String insertSQL = "INSERT INTO "+importedTable+" (";

        Iterator<Map.Entry<String, JsonNode>> metaFields = dataSJ.get("metadata").fields();
        while (metaFields.hasNext()) {
            Map.Entry<String, JsonNode> metaField = metaFields.next();
            // TODO: DBMSで分岐
            Log.out(metaField.getValue().asText());
            importedSJAttributs.add(metaField.getKey());
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
                    createSQL += metaField.getKey() + "  " + metaField.getValue().asText() + ", ";
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

        System.out.println(createSQL);

        PreparedStatement ps = conn.prepareStatement(createSQL);
        ps.executeUpdate();


        // INSERT INTO
        for (JsonNode node : dataSJ.get("data")) {
            String tmpSQL = insertSQL;
            Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
            while (nodeFields.hasNext()) {
                Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                JsonNode jn = nodeField.getValue();
                if (jn.isTextual()) {
                    tmpSQL += "'" + StringEscapeUtils.escapeSql(nodeField.getValue().asText()) +"', ";
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
            //System.out.println(tmpSQL);
            ps = conn.prepareStatement(tmpSQL);
            ps.executeUpdate();
        }
        return importedTable;
    }
}
