package rtaclient.common;

public class Log {

    private static int flag = 0; // Log.outを出力するかしないかのフラグ(デフォルトはしない)
    private static int infoflag = 1; // Log.infoを出力するかしないかのフラグ(デフォルトはする)


    public static void setLog(int f) {
        flag = f;
    }

    public static void setinfoLog(int f) {
        infoflag = f;
    }

    public static void out(Object o) {
        switch (flag) {
            case 0:
                // do nothing
                break;
            case 1:
                // ログ出力
                System.out.println(o.toString());
                break;
            case 2:
                // エラー出力
                Log.err(o.toString());
                break;
        }
    }

    // ログ出力 (-quietで出力しない)
    public static void info(Object o) {
        switch (infoflag) {
            case 0:
                // do nothing
                break;
            case 1:
                // ログ出力
                System.out.println(o.toString());
                break;
            case 2:
                // エラー出力
                Log.err(o.toString());
                break;
        }
    }

    public static void err(Object o) {
        System.err.println(o.toString());
        GlobalEnv.errorText += o.toString();
    }

}
