package rtaclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.jsonldjava.shaded.com.google.common.collect.Table;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(value = {"connector", "fromItems", "selectItems", "where", "select", "sql", "selectItem"}, ignoreUnknown=true)
public class TableConnector {
    private String dbms;
    private String host;
    private String user;
    private String password;
    @JsonProperty("dbname")
    private String dbName;
    @JsonProperty("tbname")
    private String tbName;
    @JsonProperty("accessname")
    private String accessName;
    @JsonProperty("accessmethod")
    private String accessMethod;
    @JsonProperty("sparqlQuery")
    private String sparqlQuery;
    @JsonProperty("sparqlEndpoint")
    private String sparqlEndpoint;
    @JsonProperty("sparqlColumns")
    private Map<String, String> sparqlColumns;

    private String connector = "";
    private List<FromItem> fromItems = new ArrayList<>();
    private List<SelectItem> selectItems = new ArrayList<>();
    private Expression where;
    private Select select;
    
    public TableConnector(){};

    public TableConnector(String dbms, String host, String user, String password, String dbName) {
        this.dbms = dbms;
        this.host = host;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
    }

    public TableConnector(String dbms, String host, String user, String password, String dbName, String tbName, String accessName, String accessMethod, 
   		 String sparqlQuery, String sparqlEndpoint) {
        this.dbms = dbms;
        this.host = host;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
        this.tbName = tbName;
        this.accessName = accessName;
        this.accessMethod = accessMethod;
        this.sparqlQuery = sparqlQuery;
        this.sparqlEndpoint = sparqlEndpoint;
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
    
    public void setAccessMethod(String accessMethod) {
 		this.accessMethod = accessMethod;
 	}

    public void setAccessName(String accessName) {
        this.accessName = accessName;
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
    
 	
    public void addWhere(Expression where){
    	if(this.where == null){
    		this.where = where;
    	}else{
    		OrExpression orExp = new OrExpression(this.where, where);
    		this.where = orExp;
    	}
    }
    
    public void setSparqlColumns(Map<String, String> sparqlColumns) {
 		this.sparqlColumns = sparqlColumns;
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

    public String getAccessMethod() {
		return accessMethod;
	}


	public String getAccessName() {
        return accessName;
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

    public String getSparqlQuery() {
		return sparqlQuery;
	}

	public String getSparqlEndpoint() {
		return sparqlEndpoint;
	}


	public Map<String, String> getSparqlColumns() {
		return sparqlColumns;
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
