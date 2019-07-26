package rtaserver.common;
import org.postgresql.core.v2.QueryExecutorImpl;
import rtaserver.db.DBConnect;

import java.sql.*;
import java.util.List;


public class fetchCreateSemiJoinColumnsDirectly {


    public static String fetch(List<String> importedSJAttribut, String connector, String user, String password, String dbms, String sqlQuery, String tmpdate){

        String importedTable="";
        try {
            Connection con_remote = DBConnect.connect(connector, user, password);
            Connection con_tmp = DBConnect.connectLocalTmp();

            PreparedStatement ps_remote = con_remote.prepareStatement(sqlQuery);
            ResultSet rs_remote = ps_remote.executeQuery();
            importedTable=QueryExecutor.insertFromResultSet(importedSJAttribut, dbms, con_tmp, rs_remote, tmpdate);
        } catch (SQLException e) {
            System.out.println("Error executing Remote query:" + e.toString());
            e.printStackTrace();
        }
        return importedTable;
    }


}
