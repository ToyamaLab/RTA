package rtaserver;

import rtaserver.db.DBSelectQuery;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.*;


@Path("/")
public class retrieveResult {
    @GET
    @Path("/retrieveResult")
    @Produces(MediaType.TEXT_PLAIN)
    public String sample(@QueryParam("table") String table) throws ClassNotFoundException, SQLException {
        return "{\"table_name\":\"" + retrieveSemiJoinColumns.splitSchemaFromTable(table) + DBSelectQuery.select("SELECT * FROM " + table);
    }




}
