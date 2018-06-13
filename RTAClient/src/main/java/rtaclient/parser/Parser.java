package rtaclient.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import rtaclient.common.GlobalEnv;
import rtaclient.TableConnector;
import rtaclient.common.*;
import rtaclient.db.DBConnect;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.select.*;

public class Parser {

    private Select originalSelect;
    private String original_query;
    private String tmpDateTime;
    private List<TableConnector> remoteConnector = new ArrayList<>();
    private TableConnector localConnector;

    public Parser() {
        localConnector = new TableConnector(GlobalEnv.getDriver(), GlobalEnv.getHost(), GlobalEnv.getUser(), GlobalEnv.getPassword(), GlobalEnv.getDb());
    }

    public int parse(String sql, String datetime) throws SQLException, JSQLParserException, ParserConfigurationException, FileNotFoundException, TransformerException, IOException {
        this.original_query = sql;
        this.tmpDateTime = datetime;

        // パーザーに通す前にリモートテーブルを振り分けて元のsqlから#を抜いておく
        // \\w: 半角英数字とアンダースコア \\W: 半角英数字とアンダースコア以外
        String regex = "#\\w*\\W{1}";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(original_query);

        // 先頭の#と末尾の半角スペースを消す
        while (m.find()) {
            String str = m.group();
            if (str != null && str.length() > 0) {
                str = str.substring(1, str.length() - 1);
                TableConnector tc = searchConnector(str);
                if (tc == null) {
                    return -1;
                } else {
                    tc.setAccessName(str);
                    remoteConnector.add(tc);
                }
            }
        }

        original_query = original_query.replace("#", "");

        // Parse
        if (CCJSqlParserUtil.parse(original_query).getClass().getSimpleName().equals("Select")) {
            originalSelect = (Select) CCJSqlParserUtil.parse(original_query);
        } else {
            System.out.println("RTA supports only SELECT statements.");
            return -1;
        }

        setSelectBody(originalSelect.getSelectBody());

        // generate SQL
        Log.out("");
        int tcSize = remoteConnector.size();
        for (int i = 0; i < tcSize; i++) {
            TableConnector tc = remoteConnector.get(i);
            Select tmpSelect = Utils.buildSelect(tc.getFromItems(), tc.getSelectItem(), tc.getWhere());
            tc.setSelect(tmpSelect);
            Log.out("RemoteQuery[" + i + "]: " + tmpSelect);
        }

        Select localSelect = Utils.buildSelect(localConnector.getFromItems(), localConnector.getSelectItem(), localConnector.getWhere());
        localConnector.setSelect(localSelect);

        if (!localSelect.toString().equals("null")) Log.out("LocalQuery: " + localSelect);

        Log.out("");

        return 0;
    }

    public String getTmpDateTime() {
        return tmpDateTime;
    }

    public Select getOriginalSelect() {
        return originalSelect;
    }

    public String getOriginalQuery() {
        return original_query;
    }

    public TableConnector getLocalConnector() {
        return localConnector;
    }

    public List<TableConnector> getRemoteConnector() {
        return remoteConnector;
    }

    public void setSelectBody(SelectBody sb) {
        switch (sb.getClass().getSimpleName()) {
            case "SetOperationList":
                SetOperationList sol = (SetOperationList) sb;
                List<SelectBody> sbs = sol.getSelects();
                for (SelectBody selectBody : sbs) {
                    setSelectBody(selectBody);
                }
                break;

            case "PlainSelect":
                PlainSelect pl = (PlainSelect) sb;
                setPlainSelect(pl);
                break;

            case "WithItem":
                // TODO: implement WITH clause
                break;
        }

    }

    public void setPlainSelect(PlainSelect pl) {
        setFrom(pl);
        setSelectItem(pl.getSelectItems());
        setWhere(pl.getWhere());
        // NOTE: We have to consider DISTINCT, INTO, GROUP BY, ORDER, HAVING, LIMIT, OFFSET,...
    }

    public void setFrom(PlainSelect pl) {
        FromItem fi = pl.getFromItem();
        if (fi != null) {
            setNestFrom(fi);

            if (pl.getJoins() != null) {
                List<Join> joins = pl.getJoins();
                for (Join join : joins) {
                    setFromJoin(join);
                }
            }
        }
    }

    public void setNestFrom(FromItem fi) {
        switch (fi.getClass().getSimpleName()) {
            case "Table":
                Table tb = (Table) fi;
                boolean remoteFound = false;
                boolean alreadyExists = false;
                // remoteConnectorに同じアクセス名があったらFromItemを追加
                for (TableConnector tc : remoteConnector) {
                    if (tc.getAccessName().equals(tb.getName())) {
                        for (FromItem f : tc.getFromItems()) {
                            Table t = (Table) f;
                            if (t.getName().equals(tb.getName())) {
                                alreadyExists = true;
                            }
                        }
                        if (!alreadyExists) {
                            tc.addFromItems(tb);
                        }

                        remoteFound = true;
                        break;
                    }
                }
                if (!remoteFound) {
                    switch (GlobalEnv.getDriver()) {
                        case "mysql":
                            tb.setSchemaName(GlobalEnv.getDb());
                            break;

                        default:
                            break;
                    }
                    localConnector.addFromItems(tb);
                }

                break;
            case "SubSelect":
                SubSelect ss = (SubSelect) fi;
                SelectBody sb = ss.getSelectBody();
                setSelectBody(sb);
                // TODO: 副問い合わせ時の処理
                break;

            case "SubJoin":
                SubJoin sj = (SubJoin) fi;
                Join j = sj.getJoin();
                // TODO: 副問い合わせ時の処理
                setNestFrom(sj.getLeft());
                setFromJoin(j);
                break;

            case "ValuesList":
                break;

            case "TableFunction":
                break;
        }
    }

    public void setFromJoin(Join join) {
        // MEMO: JOINの種類で場合分けする際はjoin.isaLeft()等を用いる
        FromItem fij = join.getRightItem();
        setNestFrom(fij);

        if (join.getOnExpression() != null) {
            setNestExpression(join.getOnExpression());
        }
    }

    public void setSelectItem(List<SelectItem> sis) {
        for (SelectItem si : sis) {
            switch (si.getClass().getSimpleName().toString()) {
                case "AllColumns": /* SELECT * FROM ... */
                    for (TableConnector tc : remoteConnector) {
                        tc.addSelectItems(si);
                    }
                    localConnector.addSelectItems(si);

                    break;

                case "AllTableColumns": /* SELECT TableName.* FROM ... */
                    // TODO: Implement
                    break;

                case "SelectExpressionItem":
                    SelectExpressionItem sei = (SelectExpressionItem) si;
                    setNestSelect(sei);

                    break;
            }
        }
    }

    public void setNestSelect(SelectExpressionItem sl) {
        Expression vn = sl.getExpression();
        setNestExpression(vn);
    }

    public void setWhere(Expression where) {
        if (where != null) {
            setNestExpression(where);
        }
    }

    public void setNestExpression(Expression ex) {
        if (ex != null) {
            switch (ex.getClass().getSimpleName()) {
                case "Parenthsis": /* "(" expression ")" */
                    System.out.println(ex.getClass());
                    Parenthesis parenthesis = (Parenthesis) ex;
                    Expression pthesis = parenthesis.getExpression();
                    setNestExpression(pthesis);
                    break;

                case "AndExpression":
                    AndExpression andNode = (AndExpression) ex;
                    // 左辺
                    Expression leftNode = andNode.getLeftExpression();
                    // 右辺
                    Expression rightNode = andNode.getRightExpression();
                    setNestExpression(leftNode);
                    setNestExpression(rightNode);
                    break;

                case "OrExpression":
                    OrExpression orNode = (OrExpression) ex;
                    // 左辺
                    Expression orLeftNode = orNode.getLeftExpression();
                    // 右辺
                    Expression orRightNode = orNode.getRightExpression();
                    setNestExpression(orLeftNode);
                    setNestExpression(orRightNode);
                    break;

                /* =, >=, >, <=, <, !=, LIKE */
                case "EqualsTo":
                case "GreaterThanEquals":
                case "GreaterThan":
                case "MinorThanEquals":
                case "MinorThan":
                case "NotEqualsTo":
                case "LikeExpression":
                    BinaryExpression beNode = (BinaryExpression) ex;

                    separateExpression(beNode);

                    Expression beleftNode = beNode.getLeftExpression();
                    Expression berightNode = beNode.getRightExpression();
                    setNestExpression(beleftNode);
                    setNestExpression(berightNode);
                    break;

                /* +, &, |, ^, ||, /, %, *, - */
                case "Addition":
                case "BitwiseAnd":
                case "BitwiseOr":
                case "BitwiseXor":
                case "Concat":
                case "Division":
                case "Modulo":
                case "Multiplication":
                case "Subtraction":
                    BinaryExpression binaryExpression = (BinaryExpression) ex;
                    Expression beLeftExpression = binaryExpression.getLeftExpression();
                    Expression beRightExpression = binaryExpression.getRightExpression();
                    setNestExpression(beLeftExpression);
                    setNestExpression(beRightExpression);
                    break;

                /* WHERE A BETWEEN expr1 AND expr2 */
                case "Between":
                    Between between = (Between) ex;
                    Expression btLeftExpression = between.getLeftExpression();
                    Expression btStartExpression = between.getBetweenExpressionStart();
                    Expression btEndExpression = between.getBetweenExpressionEnd();
                    setNestExpression(btLeftExpression);
                    setNestExpression(btStartExpression);
                    setNestExpression(btEndExpression);
                    break;

                /* WHERE A [NOT] IN (expr1,expr2,expr3,...) */
                case "InExpression":
                    InExpression inExpression = (InExpression) ex;
                    Expression inLeftExpression = inExpression.getLeftExpression();
                    setNestExpression(inLeftExpression);
                    break;

                /* WHERE A [NOT] EXISTS (Subquery) */
                case "ExistsExpression":
                    ExistsExpression existsExpression = (ExistsExpression) ex;
                    Expression eeRightExpression = existsExpression.getRightExpression();
                    setNestExpression(eeRightExpression);
                    break;

                case "SubSelect":
                    SubSelect subselect = (SubSelect) ex;
                    SelectBody sb = subselect.getSelectBody();
                    setSelectBody(sb);
                    break;

                case "Column":
                    Column col = (Column) ex;
                    String tableName = null;
                    if (!col.getTable().toString().equals("")) {
                        // ex) a.name → tableName: a
                        tableName = col.getTable().toString();
                        SelectExpressionItem item = new SelectExpressionItem(col);

                        boolean remoteFound = false; /* そのカラムがRemoteのものかどうか */
                        boolean alreadyExists = false; /* そのカラムが既にSELECT文に入っているかどうか */

                        // remoteConnectorから対応するカラムを探す
                        for (TableConnector tc : remoteConnector) {
                            if (tableName.equals(tc.getFromItems().get(0).getAlias().getName())) {
                                remoteFound = true;
                                alreadyExists = checkDuplicate(tc, col);
                                if (!alreadyExists) tc.addSelectItems(item);
                            }
                        }
                        // どのリモートのカラムでも無かったらローカルに
                        if (!remoteFound && !checkDuplicate(localConnector, col)) localConnector.addSelectItems(item);
                    }

                    break;

                case "Function":
                    Function fc = (Function) ex;
                    List<Expression> fexs = fc.getParameters().getExpressions();
                    for (Expression fe : fexs) {
                        setNestExpression(fe);
                    }
                    break;

                default:
                    Log.out("Expression " + ex.getClass().getSimpleName() + " isn't supported.");
                    break;
            }
        }
    }

    public boolean checkDuplicate(TableConnector tc, Column col) {
        for (SelectItem si : tc.getSelectItem()) {
            if (si.toString().equals(col.toString())) {
                return true;
            }
        }
        return false;
    }

    private int separateExpression(BinaryExpression ex) {
        boolean leftIsColumn = false, rightIsColumn = false;
        leftIsColumn = isColumn(ex.getLeftExpression());
        rightIsColumn = isColumn(ex.getRightExpression());

        int leftIndex = -1, rightIndex = -1;
        if (leftIsColumn) leftIndex = getMatchRemoteIndex((Column) ex.getLeftExpression());
        if (rightIsColumn) rightIndex = getMatchRemoteIndex((Column) ex.getRightExpression());

        if (!leftIsColumn && rightIsColumn && rightIndex!=-1) {
            remoteConnector.get(rightIndex).addWhere(ex);
        } else if (leftIsColumn && !rightIsColumn && leftIndex!=-1 ) {
            remoteConnector.get(leftIndex).addWhere(ex);
        } else if (leftIsColumn && rightIsColumn) {
            if (leftIndex < 0 || rightIndex < 0) {
            } else if (leftIndex == rightIndex) {
                remoteConnector.get(leftIndex).addWhere(ex);
            }
        }
       
        return 1;
    }

    private boolean isColumn(Expression ex) {
        if (ex.getClass().getSimpleName().equals("Column")) {
            return true;
        } else {
            return false;
        }
    }

    private int getMatchRemoteIndex(Column col) {
        String tableName = col.getTable().getName();
        int i = 0;
        for (TableConnector tc : remoteConnector) {
            Table t = (Table) tc.getFromItems().get(0);
            if (tableName.equals(t.getAlias().getName())) {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    public TableConnector searchConnector(String accessName) throws SQLException, FileNotFoundException, IOException {
        String home = GlobalEnv.USER_HOME;
        String OS_FS = GlobalEnv.OS_FS;
        String dirPath = home + OS_FS + "rta" + OS_FS + "library";
        String filePath = dirPath + OS_FS + accessName + ".rta";
        File dir = new File(dirPath);
        File file = new File(filePath);
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        if (!dir.exists()) {
            if(!dir.mkdirs()) {
                Log.out("Fail to create RTA directory.\n");
            }
        }

        if (file.exists()) {
            FileReader rtaFile = new FileReader(filePath);
            Log.out(accessName + ".rta found in " + filePath);
            TableConnector tc = mapper.readValue(rtaFile, TableConnector.class);
            rtaFile.close();
            return tc;
        }

        Log.out(accessName + ".rta does not found.");
        Connection con;
        PreparedStatement pstmt;
        ResultSet rs;
        TableConnector tc = null;

        con = DBConnect.connectLibrary();
        pstmt = con.prepareStatement("SELECT * FROM dbinfo WHERE table_name = ?");
        pstmt.setString(1, accessName);
        rs = pstmt.executeQuery();

        if (rs.next()) {
            tc = new TableConnector(
                    rs.getString("dbms"), rs.getString("host"), rs.getString("user_name"),
                    rs.getString("password"), rs.getString("db_name"), rs.getString("table_name"),
                    rs.getString("access_name"), rs.getString("access_method")

            );
            String json = mapper.writeValueAsString(tc);
            FileWriter filewriter = new FileWriter(filePath);
            filewriter.write(json);
            filewriter.close();
            Log.out("Download " + accessName + ".rta in " + filePath);

        } else {
            System.out.println("Access name " + accessName + " doesn't exist.");
        }

        DBConnect.close(con);

        return tc;
    }
}
