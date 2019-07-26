package rtaserver;

import rtaserver.common.GlobalEnv;
import rtaserver.db.DBConnect;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Path("/")
public class tableInfo {
    @GET
    @Path("/getTableInfo")
    @Produces(MediaType.TEXT_PLAIN)
    public String sample(@QueryParam("access_name") String access_name) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection con = DriverManager.getConnection("jdbc:postgresql://localhost/rta_databases?useUnicode=true&characterEncoding=utf8", "shu", "shu");
        PreparedStatement pstmt = con.prepareStatement("SELECT * FROM dbinfo WHERE table_name = ?");
        pstmt.setString(1, access_name);
        ResultSet rs = pstmt.executeQuery();
        String host = null;
        if(rs.next()){
            host = rs.getString("host");
        }else{
            System.out.println("Access name " + access_name + " doesn't exist.");
        }
        return host;
    }
}