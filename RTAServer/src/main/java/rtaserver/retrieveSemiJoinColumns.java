package rtaserver;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import rtaserver.db.DBSelectQuery;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


@Path("/")
public class retrieveSemiJoinColumns {
    @POST
    @Path("/retrieveSemiJoinColumns")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)

    /**
     *  listArgument = [ table_name, join_attribut1, join_attribut2 ... ]
     */
    public String sample(String rs_json) throws ClassNotFoundException, SQLException {

        System.out.println("retrieve SJ Column : "+ rs_json);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(rs_json);
        } catch (IOException e) {
            e.printStackTrace();
            return "error_in_query : "+e.toString();
        }

        String res="{\"not_on_site_table_name\":\""+splitSchemaFromTable(root.get("not_on_site_table_name").asText());
        String createSQL="SELECT DISTINCT ";

        for (JsonNode node : root.get("not_on_site_join_attributs")) {
            Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
            while (nodeFields.hasNext()) {
                Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                createSQL+= nodeField.getValue().asText() +",";
            }
        }

        createSQL=createSQL.substring(0,createSQL.length()-1)+" FROM "+ root.get("not_on_site_table_name").asText();

        System.out.println(createSQL);
        return res + DBSelectQuery.select(createSQL);
    }

    public static String splitSchemaFromTable(String s){
        for (int i=0; i<s.length();++i){
            if (s.charAt(i)=='.'){
                return s.substring(i+1);
            }
        }
        return s;
    }
}
