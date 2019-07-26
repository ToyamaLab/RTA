package rtaserver.db;

import java.sql.*;

import rtaserver.common.*;

public class DBConnect {

    public static Connection connect(String host, String user, String password) {

        Connection con = null;

        try {
            con = DriverManager.getConnection(host, user, password);
        } catch (SQLException e) {
            System.out.println("Error connecting to the database:");
            e.printStackTrace();
        }

        return con;
    }

    // configファイルに書かれたローカルDBに接続
    public static Connection connectLocal() {

        String url = "";
        switch (GlobalEnv.getDriver()) {
            case "sqlite":
                url = createDBURL(GlobalEnv.getDriver(), null, GlobalEnv.getDb());
                return connect(url, GlobalEnv.getUser(), GlobalEnv.getPassword());

            default:
                url = createDBURL(GlobalEnv.getDriver(), GlobalEnv.getHost(), GlobalEnv.getDb());
                return connect(url, GlobalEnv.getUser(), GlobalEnv.getPassword());
        }
    }

    // configファイルに書かれたローカルDBに接続
    public static Connection connectLocalTmp() {

        String url = "";
        switch (GlobalEnv.getDriver()) {
            case "sqlite":
                url = createDBURL(GlobalEnv.getDriver(), null, GlobalEnv.getTmpdb());
                return connect(url, GlobalEnv.getUser(), GlobalEnv.getPassword());

            case "postgresql":
                return connectLocal();

            default:
                url = createDBURL(GlobalEnv.getDriver(), GlobalEnv.getHost(), GlobalEnv.getTmpdb());
                return connect(url, GlobalEnv.getUser(), GlobalEnv.getPassword());
        }
    }

    public static Connection connectLibrary() {
        String url = createDBURL("postgresql", "131.113.101.113", "rta_databases");
        return connect(url, "shu", "shu");
    }

    public static Connection connectVPN() {
        String url = createDBURL("mysql", "127.0.0.1:3307", "tpcc");
        return connect(url, "shu", "shu");
    }

    public static void close(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                System.out.println("Error while closing database connection:");
                e.printStackTrace();
            }
        }
    }

    // jdbcDriver用のurlを作成
    public static String createDBURL(String dbms, String host, String db_name) {
        String url = "";
        url += "jdbc:";
        url += dbms + ":";
        if (host != null) {
            url += "//";
            url += host;
        }
        if (db_name != null) url += "/" + db_name;
        // TODO: 指定した文字コードで接続出来るようにする
        url += "?useUnicode=true&characterEncoding=utf8";
        return url;
    }
}
