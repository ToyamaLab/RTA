package rtaclient;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

@JsonIgnoreProperties(value = {"connector", "fromItems", "selectItems", "where", "select", "sql", "selectItem"}, ignoreUnknown=true)
public class TableConnector {
    private String dbms;
    private String host;
    private String user;
    private String password;
    @JsonProperty("dbname")
    private String dbName;
    @JsonProperty("tbnames")
    private List<String> tbNames = new ArrayList<String>();
    @JsonProperty("accessnames")
    private List<String> accessNames = new ArrayList<String>();
    @JsonProperty("accessmethod")
    private String accessMethod;

    private String connector = "";
    private List<FromItem> fromItems = new ArrayList<>();
    private List<SelectItem> selectItems = new ArrayList<>();
    private Expression where;
    private Select select;

    public TableConnector() {
    }

    public TableConnector(String host) {
        this.host = host;
    }

    public TableConnector(String dbms, String host, String user, String password, String dbName) {
        this.dbms = dbms;
        this.host = host;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
    }

    public TableConnector(String dbms, String host, String user, String password, String dbName, String tbName, String accessName, String accessMethod) {
        this.dbms = dbms;
        this.host = host;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.tbNames.add(tbName);
        this.accessNames.add(accessName);
        this.accessMethod = accessMethod;
    }

    public void setDBMS(String dbms) {
        this.dbms = dbms;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setAccessName(String accessName) {
        this.accessNames.add(accessName);
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSelect(Select select) {
        this.select = select;
    }

    public void addFromItems(FromItem fromItem) {
        this.fromItems.add(fromItem);
    }

    public void addSelectItems(SelectItem selectItem) {
        this.selectItems.add(selectItem);
    }

    public void setWhere(Expression where) {
        this.where = where;
    }

    public void setTables(List<String> tbNames) {
        this.tbNames = tbNames;
    }

    public void addTables(List<String> tbNames) {
        for(int i=0;i<tbNames.size();i++){
        	this.tbNames.add(tbNames.get(i));
        }
    }
    public void addTable(String tbName) {
        this.tbNames.add(tbName);
    }

    public void addAccessName(String accessName) {
    	this.accessNames.add(accessName);
    }

    public void addAccessNames(List<String> accessNames) {
    	for(int i=0;i<accessNames.size();i++){
        	this.accessNames.add(accessNames.get(i));
        }
    }

    public List<String> getTables() {
        return tbNames;
    }

    public Select getSelect() {
        return select;
    }

    public String getDBMS() {
        return dbms;
    }

    public String getHost() {
        return host;
    }

    public String getDbName() {
        return dbName;
    }

    public List<String> getAccessNames() {
        return accessNames;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getSQL() {
        return select.toString();
    }

    public List<FromItem> getFromItems() {
        return fromItems;
    }

    public List<SelectItem> getSelectItem() {
        return selectItems;
    }

    public Expression getWhere() {
        return where;
    }

    public String createConnector() {
        connector += "jdbc:";
        connector += dbms;
        connector += "://";
        connector += host;
        if (dbName != null) connector += "/" + dbName;
        // TODO: 指定されたエンコードにするようにする
        connector += "?";
        connector += "useUnicode=true&charcterEncoding=utf8";

        return connector;
    }
}
