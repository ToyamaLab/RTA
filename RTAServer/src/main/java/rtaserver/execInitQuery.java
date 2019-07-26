package rtaserver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import rtaserver.common.GlobalEnv;
import rtaserver.db.DBConnect;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import javax.ws.rs.core.MediaType;


import java.util.Date;
import java.util.Iterator;
import java.util.Map;


@Path("/")
public class execInitQuery {

	@POST
	@Path("/execInitQuery")
	@Produces(MediaType.TEXT_PLAIN)
	public String sample(String rs_json) throws ClassNotFoundException, SQLException {

		// JSONの解析
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = null;
		try {
			root = mapper.readTree(rs_json);
		} catch (IOException e) {
			e.printStackTrace();
		}



		String table=root.get("table").asText();
		String query = root.get("query").asText();

		if (table==""){
			return "error_in_table";
		}

		System.out.println("Exec Init");

		GlobalEnv.setGlobalEnv();
		Connection conn = null;

		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
		String tmpdate = sdf.format(date);
		String createSQL="CREATE TABLE ";
		String tmp_table="";

		switch (GlobalEnv.getDriver()) {
			case "mysql":
				Class.forName("com.mysql.jdbc.Driver");
				conn = DBConnect.connectLocalTmp();
				tmp_table =  table + "_" + tmpdate;
				createSQL += tmp_table + " AS ";
				break;
			case "postgresql":
				Class.forName("org.postgresql.Driver");
				conn = DBConnect.connectLocal();
				tmp_table = GlobalEnv.getTmpdb() + "."+ table + "_" + tmpdate;
				createSQL += tmp_table + " AS ";
				break;

			//did not implement the case sqlite
			case "sqlite":
				conn = DBConnect.connectLocalTmp();
				Statement s = conn.createStatement();
				s.execute("ATTACH \"" + GlobalEnv.getDb() + "\" as local");
				tmp_table = GlobalEnv.getTmpdb() + "."+ table + "_" + tmpdate;
				createSQL += tmp_table + " AS ";

			default:
				break;
		}

		createSQL += query;
		System.out.println(createSQL);

		try{
			PreparedStatement ps = conn.prepareStatement(createSQL);
			ps.executeUpdate();
		}  catch (SQLException e){
			e.printStackTrace();
			return "error_in_query : " + e.toString();
		}

		String sqlQuery = "SELECT 'table_init' AS \"table\",'table' AS \"set\",COUNT(*) FROM "+table+" UNION SELECT 'table_after_select' AS table, 'table' AS \"set\",COUNT(*) FROM "+tmp_table;
		for (JsonNode node : root.get("table_attributs")) {
			Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
			while (nodeFields.hasNext()) {
				Map.Entry<String, JsonNode> nodeField = nodeFields.next();
				sqlQuery += " UNION SELECT 'table_init' AS \"table\",'"+selectName(nodeField.getValue().asText())+"' AS \"set\",COUNT(DISTINCT "+selectName(nodeField.getValue().asText())+") FROM "+table;
				sqlQuery += " UNION SELECT 'table_after_select' AS \"table\",'"+selectName(nodeField.getValue().asText())+"' AS \"set\",COUNT (DISTINCT "+selectName(nodeField.getValue().asText())+") FROM "+tmp_table;
			}
		}

		String return_data="{";
		String return_cardi_init = "[";
		String return_cardi_after_select = "[";

		ResultSet rs;
		try{
			PreparedStatement ps = conn.prepareStatement(sqlQuery);
			rs = ps.executeQuery();

			while(rs.next()) {
				switch (rs.getString(1)) {
					case "table_init":
						switch (rs.getString(2)){
							case "table":
								return_data += "\"cardinality_tbl_init\":"+ rs.getInt(3)+",";
								break;
							default:
								return_cardi_init += "{\"att\":\"" + rs.getString(2) + "\",\"cardi\":" + rs.getInt(3) + "},";
						}
						break;
					case "table_after_select":
						switch (rs.getString(2)){
							case "table":
								return_data += "\"cardinality_tbl_after_select\":"+ rs.getInt(3)+",";
								break;
							default:
								return_cardi_after_select += "{\"att\":\"" + rs.getString(2) + "\",\"cardi\":" + rs.getInt(3) + "},";
						}
				}
			}
			return_data += "\"table\":\""+ tmp_table  +"\",";

			return_data += "\"cardinality_att_init\":"+ return_cardi_init.substring(0,return_cardi_init.length()-1)
					+"],\"cardinality_att_after_select\":" +return_cardi_after_select.substring(0,return_cardi_after_select.length()-1)+"]}";

		} catch (SQLException e){
			e.printStackTrace();
			return "error_in_query : "+e.toString();
		}

		return return_data;
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

