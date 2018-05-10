package rtaclient.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
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
            body.setFromItem(table);
            if (where != null) body.setWhere(where);
            select.setSelectBody(body);
        }

        return select;
    }
}
