package rtaclient.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.E_Divide;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_Multiply;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_StrConcat;
import org.apache.jena.sparql.expr.E_Subtract;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.omg.CORBA.PRIVATE_MEMBER;

import net.sf.jsqlparser.schema.Column;
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
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

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
				}		 
   		 }else{//*のため
   			 columnNames.clear();
   		 }
   	 }
   	 
   	 if(!vars.isEmpty()){
   		 op = new OpProject(op, vars);
   	 }
   	 if(where != null){
   		 op = OpFilter.filter(toExpr(where), op);
   	 }
   	 
   	 org.apache.jena.query.Query newQuery = OpAsQuery.asQuery(op);
   	 String sparqlQuery = newQuery.serialize();
   	 Log.out(sparqlQuery);
   	 
   	 return sparqlQuery;
    }   
    
    private static Expr toExpr(Expression sql) {
   	 Expr sparql = null;
   	   if(sql instanceof OrExpression){
   	   	OrExpression sqlCast = (OrExpression)sql;
   	   	sparql = new E_LogicalOr(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if (sql instanceof AndExpression){
   	   	AndExpression sqlCast = (AndExpression)sql;
   	   	sparql = new E_LogicalAnd(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if (sql instanceof EqualsTo){
   	   	EqualsTo sqlCast = (EqualsTo)sql;
   	   	sparql = new E_Equals(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if (sql instanceof NotEqualsTo){
   	   	NotEqualsTo sqlCast = (NotEqualsTo)sql;
   	   	sparql = new E_NotEquals(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if (sql instanceof GreaterThan){
   	   	GreaterThan sqlCast = (GreaterThan)sql;
   	   	sparql = new E_GreaterThan(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof GreaterThanEquals){
   	   	GreaterThanEquals sqlCast = (GreaterThanEquals)sql;
   	   	sparql = new E_GreaterThanOrEqual(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof MinorThan){
   	   	MinorThan sqlCast = (MinorThan)sql;
   	   	sparql = new E_LessThan(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof MinorThanEquals){
   	   	MinorThanEquals sqlCast = (MinorThanEquals)sql;
   	   	sparql = new E_LessThanOrEqual(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof Addition){
   	   	Addition sqlCast = (Addition)sql;
   	   	sparql = new E_Add(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof Division){
   	   	Division sqlCast = (Division)sql;
   	   	sparql = new E_Divide(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof Multiplication){
   	   	Multiplication sqlCast = (Multiplication)sql;
   	   	sparql = new E_Multiply(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof Subtraction){
   	   	Subtraction sqlCast = (Subtraction)sql;
   	   	sparql = new E_Subtract(toExpr(sqlCast.getLeftExpression() ), toExpr(sqlCast.getRightExpression()));
   	   }else if(sql instanceof Column) {
   	   	Column sqlCast = (Column)sql;
   	   	sparql = new ExprVar(sqlCast.getColumnName());
   	   }else if(sql instanceof LongValue){
   	   	LongValue sqlCast = (LongValue)sql;
   	   	sparql = new NodeValueDouble(sqlCast.getValue());
   	   }
   	   //TODO: handle all cases
   	   return sparql;
   	 }
}
