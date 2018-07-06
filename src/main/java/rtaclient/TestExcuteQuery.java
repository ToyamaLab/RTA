package rtaclient;

import rtaclient.common.GlobalEnv;
import rtaclient.db.DBTablePrinter;

public class TestExcuteQuery {

	public static void main(String[] args) {
		GlobalEnv.setGlobalEnv(args);
		RTAClient newuser = new RTAClient();
		String query =  "SELECT p.prefecture_name, p.city_name, p.address FROM #postal_code p;";//sample query

		newuser.excuteQuery(query);// TODO Auto-generated method stub
//		DBTablePrinter.printResultSet(newuser.rs_tmp);

	}

}
