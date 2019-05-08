package rtaclient.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import rtaclient.common.Log;
import net.sf.jsqlparser.expression.Expression;

public class Utils {
    public static Select buildSelect(List<FromItem> tables, List<SelectItem> selectItems, Expression where) {
        Select select = new Select();
        PlainSelect body = new PlainSelect();

        // TableとFromJoinを区別しない
        if (!tables.isEmpty() && !selectItems.isEmpty()) {
            Table table = (Table) tables.get(0);
            List<Join> joins = new ArrayList<>();
            int tableSize = tables.size();
            for (int i = 1; i < tableSize; i++) {
                Join join = new Join();
                join.setSimple(true);
                join.setRightItem(tables.get(i));
                joins.add(join);
            }

            body.setJoins(joins);
            body.setSelectItems(selectItems);
            for(SelectItem selectItem: selectItems){
            	if(selectItem instanceof AllColumns || selectItem instanceof AllTableColumns){
            		body.setSelectItems(Arrays.asList(selectItem));
            		break;
            	}
            }
            body.setFromItem(table);
            if (where != null) body.setWhere(where);
            select.setSelectBody(body);
        }

        return select;
    }
    
    public static String buildSparqlSelect(String originalSparqlQuery, List<FromItem> tables, 
   		 List<SelectItem> selectItems, Expression where, List<String> columnNames) {
   	 org.apache.jena.query.Query query = QueryFactory.create(originalSparqlQuery);
   	 Op op = Algebra.compile(query);
   	 List<Var> vars = new ArrayList<>();
   	 for(SelectItem sItem : selectItems){
   		 if(sItem.getClass().getSimpleName().toString().equals("SelectExpressionItem")){
   			 SelectExpressionItem sei = (SelectExpressionItem) sItem;
   			 Expression ex = sei.getExpression();
   			 if(ex.getClass().getSimpleName().equals("Column")){
   				 net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) ex;
   				 String columnName = col.getColumnName();
   				 vars.add(Var.alloc(columnName));
   				 columnNames.add(columnName);
   			 }else {
   				 columnNames.clear();
					return originalSparqlQuery;
				}		 
   		 }else{
   			 columnNames.clear();
   			 return originalSparqlQuery;
   		 }
   	 }
   	 op = new OpProject(op, vars);
   	 org.apache.jena.query.Query newQuery = OpAsQuery.asQuery(op);
   	 String sparqlQuery = newQuery.serialize();
   	 Log.out(sparqlQuery);
   	 
   	 return sparqlQuery;
    }
    
}
