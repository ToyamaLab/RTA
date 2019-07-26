package rtaserver.db;


import java.sql.*;
import java.util.ArrayList;

import rtaserver.common.GlobalEnv;

public class DBSelectQuery {

    public static String select(String querySQL) throws ClassNotFoundException, SQLException {
        GlobalEnv.setGlobalEnv();
        Connection conn = null;
        String u = "";

        switch (GlobalEnv.getDriver()) {
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


        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(querySQL);
            String name, value, s;
            ResultSetMetaData metadata;

            ArrayList<String> q_result;
            ArrayList<String> q_results = new ArrayList<String>();
            metadata = rs.getMetaData();
            int c = metadata.getColumnCount() + 1;

            ArrayList<String> type = new ArrayList<String>();

            for (int i = 1; i < c; i++) {
                switch (metadata.getColumnType(i)) {
                    case 12: //VARCHAR
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"varchar\"");
                        break;
                    case 4: //INTEGER
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"int\"");
                        break;
                    case 2: //NUMERIC
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"numeric\"");
                        break;
                    case -5: //BIGINT
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"bigint\"");
                        break;
                    case 91: //DATE
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"date\"");
                        break;
                    case 3: //DECIMAL
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"decimal\"");
                        break;
                    case 5: //SMALLINT
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"smallint\"");
                        break;
                }
            }

            String ty = type.toString();
            ty = "{" + ty.substring(1, ty.length() - 1) + "}";

            while (rs.next()) {
                q_result = new ArrayList<String>();
                for (int i = 1; i < c; i++) {
                    name = null;
                    value = null;
                    s = null;
                    switch (metadata.getColumnType(i)) {
                        case 12: //String
                            name = metadata.getColumnName(i);
                            value = rs.getString(i);
                            s = "\"" + name + "\":" + "\"" + value + "\"";
                            q_result.add(s);
                            break;
                        case 2: //numeric
                            name = metadata.getColumnName(i);
                            value = rs.getString(i);
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 4: //int
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case -5: //bigint
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 91: //date
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getDate(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 3: //DECIMAL
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 5: //SMALLINT
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                    }
                }
                String t = q_result.toString();
                t = "{" + t.substring(1, t.length() - 1) + "}";
                q_results.add(t);
            }

            u = q_results.toString();
            u = "\"," + "\"metadata\":" + ty + ",\"data\":[" + u.substring(1, u.length() - 1) + "]}";

        } catch (SQLException e) {
            e.printStackTrace();
            return "error_in_query : "+ e.toString();
        }
        return u;
    }

}
