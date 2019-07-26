package rtaserver.common;

import java.io.*;
import java.util.Hashtable;

import rtaserver.common.Log;

public class GlobalEnv {

    public static final char COMMENT_OUT_LETTER = '-'; // コメントアウト等による利用(ex: -- )

    public final static String USER_HOME = System.getProperty("user.home");
    public final static String OS = System.getProperty("os.name");
    public final static String OS_LS = System.getProperty("line.separator");
    public final static String OS_FS = System.getProperty("file.separator");
    public final static String OS_PS = System.getProperty("path.separator");
    public final static String EXE_FILE_PATH = getCurrentPath();
    public final static String USER_LANGUAGE = System.getProperty("user.language");
    public final static String USER_COUNTRY = System.getProperty("user.country");

    public final static String DEFAULT_CHARACTER_CODE = "UTF-8";

    public static String errorText = "";

    private static Hashtable<String, String> envs;

    private static String home;
    private static String driver;
    private static String host;
    private static String user;
    private static String password;
    private static String db;
    private static String tmp_db;

    // 引数のファイル名やオプション等を取得
    public static void setGlobalEnv() {

        envs = new Hashtable<String, String>();
        String key = null;



        getConfig();

        Log.out("GlobalEnv is " + envs);
    }

    public static void getConfig() {
        home = USER_HOME;

        driver = null;
        host = null;
        user = null;
        password = null;
        db = null;
        String config = getconfigfile(); // -cでconfigファイルを指定できる
        String[] c_value;

        if (config == null) {
            if (new File(home + OS_FS + ".rta").exists()) {
                config = home + OS_FS + ".rta";
            } else {
                config = home + OS_FS + "config.rta";
            }

            Log.out("offline config");
            c_value = getConfigValue(config);
        } else {
            Log.out("[GlobalEnv:getConfig] config file =" + config);
            c_value = getConfigValue(config);
        }

        if (c_value[0] == null && c_value[1] == null && c_value[2] == null
                && c_value[3] == null) {
            Log.err("No config file(" + config + ")");
            return;
        }
        try {
            if (c_value[0] != null) {
                driver = c_value[0];
            }
            if (c_value[1] != null) {
                host = c_value[1];
            }
            if (c_value[2] != null) {
                user = c_value[2];
            }
            if (c_value[3] != null) {
                password = c_value[3];
            }
            if (c_value[4] != null) {
                db = c_value[4];
            }
            if (c_value[5] != null) {
                tmp_db = c_value[5];
            }
        } catch (Exception e) {

        }

        return;
    }

    public static String seek(String key) {
        return envs.get(key);
    }

    public static String getconfigfile() {
        return seek("-c");
    }

    protected static String[] getConfigValue(String config) {

        String[] c_value = new String[6];
        BufferedReader filein = null;
        String line = new String();

        String[] con = {"driver", "host", "user", "password", "db", "tmp_db"};

        try {
            filein = new BufferedReader(new FileReader(config));
            while (true) {
                try {
                    line = filein.readLine();
                } catch (IOException e) {
                }
                if (line == null)
                    break;
                line = line.trim();
                for (int i = 0; i < 6; i++) {
                    if (line.startsWith(con[i])) {
                        c_value[i] = line.substring(line.indexOf("=") + 1)
                                .trim();
                    }
                    ;
                }
            }
            filein.close();
        } catch (FileNotFoundException e) {
            Log.err("Configuration file " + config + " not found.");
        } catch (IOException e) {
            Log.err("IOEXception error from rtaclient.common.GlobalEnv.getConfigValue.");
        }

        return c_value;
    }

    public static String getDriver() {
        return driver;
    }

    public static String getHost() {
        return host;
    }

    public static String getUser() {
        return user;
    }

    public static String getPassword() {
        return password;
    }

    public static String getDb() {
        return db;
    }

    public static String getTmpdb() {
        return tmp_db;
    }

    private static String getCurrentPath() {
        String cp = System.getProperty("java.class.path");
        if (cp.contains(OS_PS)) {
            String cps[] = cp.split(OS_PS);
            for (int i = 0; i < cps.length; i++) {
                if (cps[i].contains(OS_FS)) {
                    cp = cps[i].trim();
                    break;
                }
            }
        }
        if (cp.endsWith(".jar")) {
            cp = new File(cp).getParent();
            if (cp.endsWith("libs"))
                cp = new File(cp).getParent();
        }
        return cp;
    }
}
