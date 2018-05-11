package rtaclient.webService;

public class Filter {
	private String filter;
	private String table;
	private String column;
	private Integer num_of_elements;

	public  Filter(String filter,String table,String column,Integer num_of_elements){
		this.filter = filter;
		this.table = table;
		this.column = column;
		this.num_of_elements = num_of_elements;
	}

	public Filter() {
		// TODO 自動生成されたコンストラクター・スタブ
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public Integer getNum_of_elements() {
		return num_of_elements;
	}

	public void setNum_of_elements(Integer num_of_elements) {
		this.num_of_elements = num_of_elements;
	}
}




//package rtaserver;
//
//import javax.xml.bind.annotation.XmlRootElement;
//
//@XmlRootElement
//public class Filter {
//    /** 構造体ArrayList */
//    //static ArrayList<String> conf = new ArrayList<String>();
//    private int query_num;
//	private String column_name;
//	private String filter;
//	private int num_of_elements;
//
//	public Filter(String qn, String cn, String filter, String noe) {
//        this.query_num = Integer.parseInt(qn);
//        this.column_name = cn;
//        this.filter = filter;
//        this.num_of_elements = Integer.parseInt(noe);
//    }
//
//    public int getQueryNumber() {
//        return this.query_num;
//    }
//
//    public int getNumOfElemets() {
//        return this.num_of_elements;
//    }
//
//    public String getName() {
//        return this.column_name;
//    }
//
//    public String getFilter() {
//        return this.filter;
//    }
//}
