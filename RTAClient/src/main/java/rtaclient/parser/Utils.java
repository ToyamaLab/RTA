package rtaclient.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import rtaclient.RTAClient;
import rtaclient.TableConnector;
import rtaclient.common.GlobalEnv;

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
            body.setFromItem(table);
            if (where != null) body.setWhere(where);
            select.setSelectBody(body);
        }

        return select;
    }

    // kosaka add

    public static List<String> wheres = new ArrayList<String>();
    public static Boolean flag = false;

    public static List<TableConnector> remoteConnectorJoin(Parser parser, List<TableConnector> rc,String datetime) throws JSQLParserException{
    	PlainSelect pl = (PlainSelect)parser.getOriginalSelect().getSelectBody();
    	String query = pl.toString();
    	String regex = "\\s(where|WHERE){1,1}\\s"; //where句のあるパターン
    	Pattern p = Pattern.compile(regex);
    	Matcher m = p.matcher(query);
    	List<Table> froms = new ArrayList<>();
    	//TODO:where句がない場合の分岐を作る
    	if(m.find()!=false){
	        Expression expr = CCJSqlParserUtil.parseCondExpression(pl.getWhere().toString());
	        System.out.println("aaa:"+pl.getWhere().toString());
	        expr.accept(new ExpressionVisitorAdapter() {
	            @Override
	            protected void visitBinaryExpression(BinaryExpression expr) {
	                if (expr instanceof ComparisonOperator) {
	                    System.out.println("left=" + expr.getLeftExpression() + "  op=" +  expr.getStringExpression() + "  right=" + expr.getRightExpression());
	                    if(expr.getLeftExpression() instanceof Column && expr.getRightExpression() instanceof Column){
	                    	Column columnLeft = (Column) expr.getLeftExpression();
	                    	Column columnRight = (Column) expr.getRightExpression();
	                    	int joinCon1 = -1,joinCon2 = -1;
	                    	for(int i=0;i<rc.size();i++){
	                    		List<FromItem>  fi = rc.get(i).getFromItems();
	                    		for(int j=0;j<fi.size();j++){
			                    	if(fi.get(j).getAlias().getName().equals(columnLeft.getTable().toString())){
			                    		joinCon1 = i;
			                    	};
			                    	if(fi.get(j).getAlias().getName().equals(columnRight.getTable().toString())){
			                    		joinCon2 = i;
			                    	};
	                    		}
	                    	}
	                    	if(joinCon1 != -1 && joinCon2 != -1){
	                    		System.out.println("size1:"+rc.get(joinCon1).getTables().size()+",size2:"+rc.get(joinCon2).getTables().size());

	                    		if(rc.get(joinCon1).getHost().equals(rc.get(joinCon2).getHost())){
	                    			flag = true;
	                    			String bef = expr.getLeftExpression()+" "+expr.getStringExpression()+" "+expr.getRightExpression();
	                    			wheres.add(bef);
	                    			List<FromItem> fromItems1 = rc.get(joinCon1).getFromItems();
	                        		List<SelectItem> selectItems1 = rc.get(joinCon1).getSelectItem();

	                        		TableConnector result = rc.get(joinCon2);
	                        		PlainSelect select = (PlainSelect) result.getSelect().getSelectBody();
	                        		List<Join> joins = new ArrayList<Join>();

	                        		int j;
	                        		for(j=0;j<fromItems1.size();j++){
	                        			Join join = new Join();
	                        			join.setSimple(true);
	                        			join.setRightItem(fromItems1.get(j));
	                        			joins.add(join);
	                        			System.out.println("test1");
	                        			result.addFromItems(fromItems1.get(j));
	                        		}
	                        		if(!select.getJoins().isEmpty()){
	                        			for(j=0;j<select.getJoins().size();j++){
	                        				joins.add(select.getJoins().get(j));
	                        			}
	                        		}

	                        		select.setJoins(joins);

	                        		for(j=0;j<selectItems1.size();j++){
	                        			System.out.println("test2");
	                        			result.addSelectItems(selectItems1.get(j));
	                        		}
	                        		System.out.println("TEST:"+select.toString());
	                        		Matcher tmp = p.matcher(select.toString());
	                        		String where = "";
	                        		if(tmp.find()){
	                        			where = select.getWhere().toString() +" AND "+ expr.toString();
	                        		}else{
	                        			where = expr.toString();
	                        		}
	                        		Expression finalWhere;
									try {
										finalWhere = CCJSqlParserUtil.parseCondExpression(where);
										select.setWhere(finalWhere);
									} catch (JSQLParserException e) {
										// TODO 自動生成された catch ブロック
										e.printStackTrace();
									}

									PlainSelect pl = (PlainSelect)parser.getOriginalSelect().getSelectBody();
							        Table t = new Table();
							        t = (Table)pl.getFromItem();
							        froms.add(t);
							        for(int i=1;i<pl.getJoins().size()+1;i++){
							        	t = (Table)pl.getJoins().get(i-1).getRightItem();
							        	froms.add(t);
							        }

									List<SelectItem> sis = select.getSelectItems();

									for(int k = 0;k<sis.size();k++){
										SelectExpressionItem si = (SelectExpressionItem) sis.get(k);
										Column c = (Column) si.getExpression();
										for(int l=0;l<froms.size();l++){
											if(c.getTable().getName().equals(froms.get(l).getAlias().getName())){
												Alias alias = new Alias(froms.get(l).getName()+"_"+c.getColumnName(),true);
												si.setAlias(alias);
											}
										}
									}

	                        		result.setWhere(expr);
	                        		Select s = new Select();
	                        		s.setSelectBody(select);
	                        		result.setSelect(s);
	                        		result.addTables(rc.get(joinCon1).getTables());
	                        		result.addAccessNames(rc.get(joinCon1).getAccessNames());
	                        		rc.remove(joinCon1);
	                        		System.out.println(result.getSQL());
	                        		System.out.println("expr:"+expr.getLeftExpression()+" "+expr.getStringExpression()+" "+expr.getRightExpression());
	                    		}
	                    	}
	                    }
	                }
	                super.visitBinaryExpression(expr);
	            }
	        });

	        int i,j,k,l;
	        String tmp="",aft="";

	        if(flag){
	        	RTAClient.Flag = true;
				//originalの書き換え
		        for(i=0;i<wheres.size();i++){
		        	//where句の書き換え part1
		        	String bef = wheres.get(i);
		        	String regex1 = bef+"\\s{1,}(and|AND|or|OR){1,1}\\s"; //str and ~的なパターン
		        	String regex2 = "(and|AND|or|OR){1,1}\\s{1,}"+bef+"\\s+;"; //and str; ~的なパターン
		        	Pattern p1 = Pattern.compile(regex1);
		        	Pattern p2 = Pattern.compile(regex2);
		        	Matcher m1 = p1.matcher(pl.toString());
		            tmp = m1.replaceAll("");
		            Matcher m2 = p2.matcher(tmp);
		            aft = m2.replaceAll("");
		            System.out.println("bef:"+pl.toString());
		            System.out.println("aft:"+aft);
		        }

		        String newFrom="";
		        String newAlias="";
		        for(j=0;j<rc.size();j++){
		        	List<FromItem> f = rc.get(j).getFromItems();
		        	Table t = new Table();
		        	//form句の書き換え
		        	if(f.size()!=1){
		        		for(k=0;k<f.size();k++){
		        			t = (Table)f.get(k);
		    				//Table名およびAliasの統合
		        			if(newFrom.equals("")){
		    					newFrom = t.getName();
		    					newAlias = t.getAlias().getName();
		    				}else{
		    					newFrom = newFrom+"_"+t.getName();
		    					newAlias = newAlias+t.getAlias().getName();
		    				}
		        			System.out.println("FROM:"+newFrom);
		        			System.out.println("ALIAS:"+newAlias);
		        			if(k!=f.size()-1){
		        				System.out.println("bef2:"+aft);
		        				System.out.println("f.get(k):"+f.get(k).toString());
			        			String regex3 = ",\\s{1,}" + f.get(k).toString(); //,str ~的なパターン
			                	String regex4 = f.get(k).toString()+"\\s{1,}(where|WHERE){1,1}"; //str where ~的なパターン
			                	Pattern p3 = Pattern.compile(regex3);
			                	Pattern p4 = Pattern.compile(regex4);
			                	Matcher m3 = p3.matcher(aft);
			                    tmp = m3.replaceAll("");
			                    Matcher m4 = p4.matcher(tmp);
			                    aft = m4.replaceAll("WHERE");
			                    System.out.println("aft2:"+aft);
		        			}else{
		        				System.out.println("else_bef2:"+aft);
		        				System.out.println("else_f.get(k):"+f.get(k).toString());
		        				String regex3 = f.get(k).toString()+"\\s{0,}"; //str, ~的なパターン
			                	String regex4 = f.get(k).toString()+"\\s{1,}(where|WHERE){1,1}\\s{1,}"; //str where ~的なパターン
			                	Pattern p3 = Pattern.compile(regex3);
			                	Pattern p4 = Pattern.compile(regex4);
			                	Matcher m3 = p3.matcher(aft);
			                    tmp = m3.replaceAll(GlobalEnv.getTmpdb()+"\\."+newFrom+"_"+datetime+" "+newAlias + " ");
			                    Matcher m4 = p4.matcher(tmp);
			                    aft = m4.replaceAll(" WHERE ");
			                    System.out.println("aft2:"+aft);
		        			}
		        		}
		        	}
		        	//slect句の書き換え + where句の書き換え
		        	for(i=0;i<f.size();i++){
		        		System.out.println("bef3:"+aft);
		        		t = (Table)f.get(i);
		        		String regex5 = "\\s"+t.getAlias().getName()+"\\."; //str.~的なパターン
		            	Pattern p5 = Pattern.compile(regex5);
		            	Matcher m5 = p5.matcher(aft);
		                aft = m5.replaceAll(" "+newAlias+"\\."+t.getName()+"_");
//		            	aft = m5.replaceAll(" "+newAlias+"\\."+newFrom+"_");
		                System.out.println("aft3:"+aft);
		        	}
		        }

		        if (CCJSqlParserUtil.parse(aft).getClass().getSimpleName().equals("Select")) {
		            Select finalSelect = (Select) CCJSqlParserUtil.parse(aft);
		            RTAClient.finalSelect = finalSelect;
		        }
	        }
    	}
	        System.out.println("My Method Finished!");
	    	return rc;
    	}

	    public static void createFinalSelect(Parser parser,List<TableConnector> rc,String datetime){
	    	String aft="";
	        PlainSelect pl = (PlainSelect)parser.getOriginalSelect().getSelectBody();
	        System.out.println("bef:"+pl.toString());
	        ArrayList<String> tmp = new ArrayList<>();
			//originalの書き換え
	        for(int i=0;i<rc.size();i++){
		        List<FromItem> f = rc.get(i).getFromItems();
		        Table t = new Table();
		        for(int j=0;j<f.size();j++){
		        	//form句の書き換え
		        	t = (Table)f.get(j);
		        	tmp.add(t.getName());
		       		pl = (PlainSelect) RTAClient.changeTableTimestamp(pl, t.getName());
		        }
		        System.out.println("middle:"+pl.toString());
		       	for(int j=0;j<f.size();j++){
		       		//slect句の書き換え + where句の書き換え
		       		String original = pl.toString();
		        	String regex = "\\s"+t.getAlias().getName()+"\\."; //str.~的なパターン
		        	Pattern p = Pattern.compile(regex);
		        	Matcher m = p.matcher(original);
		        	aft = m.replaceAll(" "+t.getAlias().getName()+"\\."+tmp.get(j)+"_");
		        	System.out.println("aft:"+aft);
		        }
	        }

	        try {
	        	if (CCJSqlParserUtil.parse(aft).getClass().getSimpleName().equals("Select")) {
	        		Select finalSelect = (Select) CCJSqlParserUtil.parse(aft);
	        		System.out.println("finalSelect:"+finalSelect.toString());
	        		RTAClient.finalSelect = finalSelect;
	        	}
	        } catch (JSQLParserException e) {
	        	// TODO 自動生成された catch ブロック
	        	e.printStackTrace();
	        }
		System.out.println("My Method Finished!");
	    	return ;
	    }

	    public static String addAlias(String sql) throws JSQLParserException{
	    	System.out.println("TEST:"+sql);
	    	Select originalSelect = null;
	    	ArrayList<Table> tables = new ArrayList<>();
	    	if (CCJSqlParserUtil.parse(sql).getClass().getSimpleName().equals("Select")) {
	    		originalSelect = (Select) CCJSqlParserUtil.parse(sql);
	        }
	    	PlainSelect pl = (PlainSelect) originalSelect.getSelectBody();
	    	Table t = (Table)pl.getFromItem();
	    	tables.add(t);
	    	if(isJoins(pl)){
	    		for(int i=1;i<pl.getJoins().size()+1;i++){
			        t = (Table)pl.getJoins().get(i-1).getRightItem();
			        tables.add(t);
	    		}
	    	}
	    	List<SelectItem> sis = pl.getSelectItems();
    		for(int k = 0;k<sis.size();k++){
				SelectExpressionItem si = (SelectExpressionItem) sis.get(k);
				Column c = (Column) si.getExpression();
				for(int l=0;l<tables.size();l++){
					if(c.getTable().getName().equals(tables.get(l).getAlias().getName())){
						Alias alias = new Alias(tables.get(l).getName()+"_"+c.getColumnName(),true);
						si.setAlias(alias);
					}
				}
			}
    		pl.setSelectItems(sis);
	    	return pl.toString();
	    }

		static Boolean isJoins(PlainSelect pl){
			String q = pl.toString();
			String original = pl.toString();
	    	String regex = "\\s(from|FROM)\\s{1,}\\S{1,}\\s{1,}\\S{1,}\\s{0,},"; //str.~的なパターン
	    	Pattern p = Pattern.compile(regex);
	    	Matcher m = p.matcher(q);
	    	if(m.matches())
	    		return true;
	    	else
	    		return false;
		}

}
