import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.FormParam;

@Path("/")
public class getData {
	@GET
	@Path("/test")
	@Produces(MediaType.TEXT_PLAIN)
	public String sample(@QueryParam("table") String table,@QueryParam("query") String query) throws ClassNotFoundException, SQLException {
		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/rta_databases","shu","shu");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		String name,value,s;
		ResultSetMetaData metadata;

		ArrayList<String> q_result;
		ArrayList<String> q_results = new ArrayList<String>();
		metadata = rs.getMetaData();
		int c = metadata.getColumnCount() + 1;
		
		ArrayList<String> type = new ArrayList<String>();

		for(int i = 1;i < c;i++){
			switch(metadata.getColumnType(i)){
				case 12: //VARCHAR
					type.add("\""+metadata.getColumnName(i)+"\":"+"\"varchar\"");
					break;
				case 4: //INTEGER
					type.add("\""+metadata.getColumnName(i)+"\":"+"\"int\"");
					break;
				case 2: //NUMERIC
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"numeric\"");
                                        break;
				case -5: //BIGINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"bigint\"");
                                        break;
				case 91: //DATE
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"date\"");
			                break;
				case 3: //DECIMAL
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"decimal\"");
                                        break;
				case 5: //SMALLINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"smallint\"");
                                        break;
			}
		}

		String ty = type.toString();
		ty = "{"+ty.substring(1,ty.length()-1)+"}";

		while(rs.next()){
			q_result = new ArrayList<String>();
			for(int i = 1; i < c; i++){
				name = null; value = null; s= null;
				switch(metadata.getColumnType(i)){
					case 12: //String
						name = metadata.getColumnName(i);
						value = rs.getString(i);
						s = "\""+ name +"\":" + "\""+ value +"\"";
						q_result.add(s);
						break;
					case 2: //numeric
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
					case 4: //int
						name = metadata.getColumnName(i);
						value = String.valueOf(rs.getInt(i));
						s = "\""+ name +"\":" + value;
						q_result.add(s);
						break;
					case -5: //bigint
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
					case 91: //date
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getDate(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
					case 3: //DECIMAL
                                        	name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;	
                                	case 5: //SMALLINT
						name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
				}
			}
			String t = q_result.toString();
			t = "{"+t.substring(1,t.length()-1)+"}";
			q_results.add(t);
		}

		String u = q_results.toString();
		u = "{\"table_name\":\""+ table +"\","+"\"metadata\":"+ty+",\"data\":[" + u.substring(1,u.length()-1)+"]}";
		return u;
	}

        @GET
        @Path("/test2")
        @Produces(MediaType.TEXT_PLAIN)
        public String sample2(@QueryParam("table") String table,@QueryParam("query") String query) throws ClassNotFoundException, SQLException {
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/tpcc","root","ssql5591");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                String name,value,s;
                ResultSetMetaData metadata;

                ArrayList<String> q_result;
                ArrayList<String> q_results = new ArrayList<String>();
                metadata = rs.getMetaData();
                int c = metadata.getColumnCount() + 1;

                ArrayList<String> type = new ArrayList<String>();

                for(int i = 1;i < c;i++){
                        switch(metadata.getColumnType(i)){
                                case 12: //VARCHAR
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"varchar\"");
                                        break;
                                case 4: //INTEGER
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"int\"");
                                        break;
                                case 2: //NUMERIC
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"numeric\"");
                                        break;
                                case -5: //BIGINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"bigint\"");
                                        break;
                                case 91: //DATE
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"date\"");
                                        break;
                                case 3: //DECIMAL
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"decimal\"");
                                        break;
                                case 5: //SMALLINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"smallint\"");
                                        break;
                        }
                }

                String ty = type.toString();
                ty = "{"+ty.substring(1,ty.length()-1)+"}";

                while(rs.next()){
                        q_result = new ArrayList<String>();
                        for(int i = 1; i < c; i++){
                                name = null; value = null; s= null;
                                switch(metadata.getColumnType(i)){
                                        case 12: //String
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + "\""+ value +"\"";
                                                q_result.add(s);
                                                break;
                                        case 2: //numeric
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 4: //int
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case -5: //bigint
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 91: //date
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getDate(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 3: //DECIMAL
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 5: //SMALLINT
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                }
                        }
                        String t = q_result.toString();
                        t = "{"+t.substring(1,t.length()-1)+"}";
                        q_results.add(t);
                }

                String u = q_results.toString();
                u = "{\"table_name\":\""+ table +"\","+"\"metadata\":"+ty+",\"data\":[" + u.substring(1,u.length()-1)+"]}";
                return u;
        }
	

	@POST
        @Path("/test3")
        @Produces(MediaType.TEXT_PLAIN)
        public String sample3(@FormParam("table") String table,@FormParam("query") String query) throws ClassNotFoundException, SQLException {
                Class.forName("org.postgresql.Driver");
                Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/rta_databases","shu","shu");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                String name,value,s;
                ResultSetMetaData metadata;

                ArrayList<String> q_result;
                ArrayList<String> q_results = new ArrayList<String>();
                metadata = rs.getMetaData();
                int c = metadata.getColumnCount() + 1;

                ArrayList<String> type = new ArrayList<String>();

                for(int i = 1;i < c;i++){
                        switch(metadata.getColumnType(i)){
                                case 12: //VARCHAR
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"varchar\"");
                                        break;
                                case 4: //INTEGER
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"int\"");
                                        break;
                                case 2: //NUMERIC
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"numeric\"");
                                        break;
                                case -5: //BIGINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"bigint\"");
                                        break;
                                case 91: //DATE
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"date\"");
                                        break;
				case 3: //DECIMAL
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"decimal\"");
                                        break;
                                case 5: //SMALLINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"smallint\"");
                                        break;
                        }
                }
		String ty = type.toString();
                ty = "{"+ty.substring(1,ty.length()-1)+"}";

                while(rs.next()){
                        q_result = new ArrayList<String>();
                        for(int i = 1; i < c; i++){
                                name = null; value = null; s= null;
                                switch(metadata.getColumnType(i)){
                                        case 12: //String
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + "\""+ value +"\"";
                                                q_result.add(s);
                                                break;
                                        case 2: //numeric
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 4: //int
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case -5: //bigint
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 91: //date
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getDate(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
					case 3: //DECIMAL
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 5: //SMALLINT
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                }
                        }
                        String t = q_result.toString();
                        t = "{"+t.substring(1,t.length()-1)+"}";
                        q_results.add(t);
                }

                String u = q_results.toString();
                u = "{\"table_name\":\""+ table +"\","+"\"metadata\":"+ty+",\"data\":[" + u.substring(1,u.length()-1)+"]}";
                return u;
        }

	@POST
        @Path("/test4")
        @Produces(MediaType.TEXT_PLAIN)
        public String sample4(@FormParam("table") String table,@FormParam("query") String query) throws ClassNotFoundException, SQLException {
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/tpcc","root","ssql5591");
                Statement stmt = conn.createStatement();
		System.out.println(query);
                ResultSet rs = stmt.executeQuery(query);
                String name,value,s;
                ResultSetMetaData metadata;

                ArrayList<String> q_result;
                ArrayList<String> q_results = new ArrayList<String>();
                metadata = rs.getMetaData();
                int c = metadata.getColumnCount() + 1;

                ArrayList<String> type = new ArrayList<String>();

                for(int i = 1;i < c;i++){
                        switch(metadata.getColumnType(i)){
                                case 12: //VARCHAR
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"varchar\"");
                                        break;
                                case 4: //INTEGER
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"int\"");
                                        break;
                                case 2: //NUMERIC
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"numeric\"");
                                        break;
                                case -5: //BIGINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"bigint\"");
                                        break;
                                case 91: //DATE
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"date\"");
                                        break;
                                case 3: //DECIMAL
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"decimal\"");
                                        break;
                                case 5: //SMALLINT
                                        type.add("\""+metadata.getColumnName(i)+"\":"+"\"smallint\"");
                                        break;
                        }
                }

                String ty = type.toString();
                ty = "{"+ty.substring(1,ty.length()-1)+"}";

                while(rs.next()){
                        q_result = new ArrayList<String>();
                        for(int i = 1; i < c; i++){
                                name = null; value = null; s= null;
                                switch(metadata.getColumnType(i)){
                                        case 12: //String
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + "\""+ value +"\"";
                                                q_result.add(s);
                                                break;
                                        case 2: //numeric
                                                name = metadata.getColumnName(i);
                                                value = rs.getString(i);
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 4: //int
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case -5: //bigint
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 91: //date
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getDate(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 3: //DECIMAL
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                        case 5: //SMALLINT
                                                name = metadata.getColumnName(i);
                                                value = String.valueOf(rs.getInt(i));
                                                s = "\""+ name +"\":" + value;
                                                q_result.add(s);
                                                break;
                                }
                        }
                        String t = q_result.toString();
                        t = "{"+t.substring(1,t.length()-1)+"}";
                        q_results.add(t);
                }

                String u = q_results.toString();
                u = "{\"table_name\":\""+ table +"\","+"\"metadata\":"+ty+",\"data\":[" + u.substring(1,u.length()-1)+"]}";
                return u;
        }

}

