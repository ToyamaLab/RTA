package rtaclient.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import rtaclient.sjmanager.SemiJoinOperation;
import rtaclient.TableConnector;
import rtaclient.common.GlobalEnv;
import rtaclient.common.Log;
import rtaclient.db.DBConnect;
import rtaclient.sjmanager.attributDAGRoot;
import rtaclient.sjmanager.tableDAGRoot;


import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

    private Select originalSelect;
    private String original_query;
    private String tmpDateTime;
    private List<TableConnector> remoteConnector = new ArrayList<>();
    private List<TableConnector> localConnector = new ArrayList<>();
    private List<SemiJoinOperation> SJPlan= new ArrayList<>();
    private Map<String, tableDAGRoot> tableDAGRoots = new HashMap<>();


    public void printSJPlan(){
        for(SemiJoinOperation sjOp : SJPlan){
            sjOp.print();
        }
    }


    public Map<String, tableDAGRoot> getTableDAGRoots(){ return tableDAGRoots; }

    public List<SemiJoinOperation> getSJPlan(){
        return SJPlan;
    }

    public void executeSJOperation(int index){
        SJPlan.get(index).executeSJ(this);
    }

    public Parser() {
    }

    public void addLocalTC(String tableName){
        TableConnector tc = new TableConnector(GlobalEnv.getDriver(), GlobalEnv.getHost(), GlobalEnv.getUser(), GlobalEnv.getPassword(), GlobalEnv.getDb(),tableName,null,null);
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
            String str = m.group();  // strは#postal_codeとか
            System.out.println(str);
            if (str != null && str.length() > 0) {
                str = str.substring(1, str.length() - 1);  //#postal_code → postal_code
                TableConnector tc = searchConnector(str);  //TableConnectorはtableの情報，dbmsとかhostとか
                if (tc == null) {
                    return -1;
                } else {
                    //tc.setAccessName(str);
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

        System.out.println("DEBUT");


        setSelectBody(originalSelect.getSelectBody());

        System.out.println("FINI");

        // generate SQL
        Log.out("");
        for (int i = 0; i < remoteConnector.size(); i++) {
            TableConnector tc = remoteConnector.get(i);
            tc.print();
            Select tmpSelect = Utils.buildSelect(tc.getFromItems(), tc.getSelectItem(), tc.getWhere());
            tc.setSelect(tmpSelect);
            Log.out("RemoteQuery[" + i + "]: " + tmpSelect);
        }
        for (int i = 0; i < localConnector.size(); i++) {
            TableConnector tc = localConnector.get(i);
            tc.print();
            Select tmpSelect = Utils.buildSelect(tc.getFromItems(), tc.getSelectItem(), tc.getWhere());
            tc.setSelect(tmpSelect);
            Log.out("LocalQuery[" + i + "]: " + tmpSelect);
        }


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


    public List<TableConnector> getRemoteConnector() {
        return remoteConnector;
    }

    public List<TableConnector> getLocalConnector() {
        return localConnector;
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
                System.out.println("PlainSelect");
                System.out.println(sb);
                System.out.println(pl);
                setPlainSelect(pl);
                break;

            case "WithItem":
                // TODO: implement WITH clause
                break;
        }

    }

    public void setPlainSelect(PlainSelect pl) {
        System.out.println("<setPlainSelect>");
        setFrom(pl);
        TableConnector tc = remoteConnector.get(0);
        System.out.println("setFrom");
        tc.print();
        setSelectItem(pl.getSelectItems());
        tc = remoteConnector.get(0);
        System.out.println("setSelectItem");
        tc.print();
        setWhere(pl.getWhere());
        tc = remoteConnector.get(0);
        System.out.println("setWhere");
        tc.print();
        // NOTE: We have to consider DISTINCT, INTO, GROUP BY, ORDER, HAVING, LIMIT, OFFSET,...
        System.out.println("</setPlainSelect>");
    }



    public void setFrom(PlainSelect pl) {
        System.out.println("  <setFrom>");
        FromItem fi = pl.getFromItem();
        System.out.println("aaa"+fi);
        if (fi != null) {
            setNestFrom(fi);
            if (pl.getJoins() != null) {
                List<Join> joins = pl.getJoins();
                for (Join join : joins) {
                    System.out.println("bbb"+join);
                    setFromJoin(join);
                }
            }
        }
        System.out.println("  </setFrom>");
    }

    public void setNestFrom(FromItem fi) {
        System.out.println("    <setNestFrom>");
        System.out.println(fi.getClass().getSimpleName());
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
                            System.out.println("AAAADDDDDDD"+tb);
                            tc.addFromItems(tb);
                            remoteFound = true;
                            break;
                        }
                    }
                    alreadyExists = false;
                }
                if (!remoteFound) {
                    switch (GlobalEnv.getDriver()) {
                        case "mysql":
                            tb.setSchemaName(GlobalEnv.getDb());
                            break;

                        default:
                            break;
                    }

                    boolean localFound=false;
                    for(TableConnector tc : localConnector){
                        if (tc.getAccessName().equals(tb.getName())) {
                            localFound = true;
                        }
                    }
                    if (!localFound){
                        TableConnector tc = new TableConnector(GlobalEnv.getDriver(), GlobalEnv.getHost(), GlobalEnv.getUser(), GlobalEnv.getPassword(), GlobalEnv.getDb(),tb.getName(),null,null);
                        tc.addFromItems(tb);
                        localConnector.add(tc);
                    }
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
        System.out.println("    </setNestFrom>");
    }

    public void setFromJoin(Join join) {
        // MEMO: JOINの種類で場合分けする際はjoin.isaLeft()等を用いる
        System.out.println("     <setFromJoin>");
        FromItem fij = join.getRightItem();
        setNestFrom(fij);

        if (join.getOnExpression() != null) {
            setNestExpression(join.getOnExpression());
        }
        System.out.println("     </setFromJoin>");
    }

    public void setSelectItem(List<SelectItem> sis) {
        System.out.println("<setSelectItem>");

        for (SelectItem si : sis) {
            switch (si.getClass().getSimpleName().toString()) {
                case "AllColumns": /* SELECT * FROM ... */
                    for (TableConnector tc : remoteConnector) {
                        tc.addSelectItems(si);
                    }
                    for (TableConnector tc : localConnector) {
                        tc.addSelectItems(si);
                    }

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
        System.out.println("</setSelectItem>");
    }

    public void setNestSelect(SelectExpressionItem sl) {
        Expression vn = sl.getExpression();
        System.out.println("getExpression : "+ vn);
        setNestExpression(vn);
    }

    public void setWhere(Expression where) {
        if (where != null) {
            setNestExpression(where);
        }
    }

    public void setNestExpression(Expression ex) {
        if (ex != null) {
            System.out.println("setNestExpression : "+ ex.getClass().getSimpleName());
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
                        if (!remoteFound){
                            for (TableConnector tc : localConnector) {
                                if (tableName.equals(tc.getFromItems().get(0).getAlias().getName())) {
                                    alreadyExists = checkDuplicate(tc, col);
                                    if (!alreadyExists) tc.addSelectItems(item);
                                }
                            }
                        }
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

    /**
     *  The where condition are added to the proper remoteConnector and eventuel SJ operations are set
     */
    private int separateExpression(BinaryExpression ex) {
        System.out.println("separateExpression : "+ex);
        boolean leftIsColumn = false, rightIsColumn = false;
        leftIsColumn = isColumn(ex.getLeftExpression());
        rightIsColumn = isColumn(ex.getRightExpression());

        int leftIndex = -1, rightIndex = -1;
        if (leftIsColumn) leftIndex = getMatchRemoteIndex((Column) ex.getLeftExpression(),remoteConnector);
        if (rightIsColumn) rightIndex = getMatchRemoteIndex((Column) ex.getRightExpression(),remoteConnector);



        if (!leftIsColumn && rightIsColumn && rightIndex!=-1) { // value=att(remote)
            remoteConnector.get(rightIndex).addWhere(ex);
        } else if (leftIsColumn && !rightIsColumn && leftIndex!=-1 ) { // att(remote)=value
            remoteConnector.get(leftIndex).addWhere(ex);
        }if (!leftIsColumn && rightIsColumn && rightIndex==-1) { // value=att(local)
            rightIndex = getMatchRemoteIndex((Column) ex.getRightExpression(),localConnector);// if the tc is local (index<0), the real Local index is fetched
            localConnector.get(rightIndex).addWhere(ex);
        } else if (leftIsColumn && !rightIsColumn && leftIndex==-1 ) { // att(local)=value
            leftIndex = getMatchRemoteIndex((Column) ex.getLeftExpression(),localConnector);// if the tc is local (index<0), the real Local index is fetched
            localConnector.get(leftIndex).addWhere(ex);
        } else if (leftIsColumn && rightIsColumn) {
            if (leftIndex < 0 && rightIndex < 0) { // att(local)=att(local)
            } else if (leftIndex<0){ // att(local)=att(remote)
                //if (true) the attributs will be add in the .addAttributToExistingSJop Methode
                leftIndex = getMatchRemoteIndex((Column) ex.getLeftExpression(),localConnector);  // if the tc is local (index<0), the real Local index is fetched
                if (!addAttributToExistingSJOp(remoteConnector.get(rightIndex),localConnector.get(leftIndex),ex.getRightExpression(),ex.getLeftExpression())){
                    SemiJoinOperation op = new SemiJoinOperation(remoteConnector.get(rightIndex),localConnector.get(leftIndex),ex.getRightExpression(),ex.getLeftExpression(),true);
                    SJPlan.add(op);
                }
            } else if (rightIndex<0){ // att(remote)=att(local)
                rightIndex = getMatchRemoteIndex((Column) ex.getRightExpression(),localConnector);// if the tc is local (index<0), the real Local index is fetched
                if (!addAttributToExistingSJOp(remoteConnector.get(leftIndex),localConnector.get(rightIndex),ex.getLeftExpression(),ex.getRightExpression())){
                    SemiJoinOperation op = new SemiJoinOperation(remoteConnector.get(leftIndex),localConnector.get(rightIndex),ex.getLeftExpression(),ex.getRightExpression(),true);
                    SJPlan.add(op);
                }
            } else if (leftIndex == rightIndex) { // att(remoteI)=att(remoteI)
                remoteConnector.get(leftIndex).addWhere(ex);
            } else{ // att(remoteJ)=att(remoteI)
                 if (!addAttributToExistingSJOp(remoteConnector.get(leftIndex),remoteConnector.get(rightIndex),ex.getLeftExpression(),ex.getRightExpression())){
                    SemiJoinOperation op = new SemiJoinOperation(remoteConnector.get(rightIndex),remoteConnector.get(leftIndex),ex.getRightExpression(),ex.getLeftExpression(),false);
                    SJPlan.add(op);
                    op = new SemiJoinOperation(remoteConnector.get(leftIndex),remoteConnector.get(rightIndex),ex.getLeftExpression(),ex.getRightExpression(),false);
                    SJPlan.add(op);
                }
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

    private int getMatchRemoteIndex(Column col, List<TableConnector> tcSet) {
        String tableName = col.getTable().getName();
        int i = 0;
        System.out.println("table_name " + tableName);
        for (TableConnector tc : tcSet) {
            Table t = (Table) tc.getFromItems().get(0);
            System.out.println(t.getAlias().getName());
            if (tableName.equals(t.getAlias().getName())) {
                System.out.println("return");
                return i;
            }
            i++;
        }
        System.out.println("return -1 ");
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
            tc.print();
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

    public boolean addAttributToExistingSJOp(TableConnector leftTC,TableConnector rightTC, Expression leftEx, Expression rightEx ){
        boolean alreadyExist=false;
        for(SemiJoinOperation sj : SJPlan){
            if (sj.checkAlreadyExist(leftTC,rightTC,leftEx, rightEx)) {
                alreadyExist=true;
            }
        }
        return alreadyExist;
    }
}
