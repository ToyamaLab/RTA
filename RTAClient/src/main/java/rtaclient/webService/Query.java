package rtaclient.webService;


import java.util.ArrayList;


public class Query {
	private Integer ID;
	private String Query;
	private ArrayList<Filter> Filters;

	public Query(Integer id,String query,ArrayList<Filter> filters){
		this.setID(id);
		this.setQuery(query);
		this.setFilters(filters);
	}

	public Query(Integer id,String query){
		this.setID(id);
		this.setQuery(query);
		this.Filters = new ArrayList<>();
	}

	public Integer getID() {
		return ID;
	}

	public void setID(Integer id) {
		this.ID = id;
	}

	public String getQuery() {
		return Query;
	}

	public void setQuery(String query) {
		this.Query = query;
	}

	public ArrayList<Filter> getFilters() {
		return Filters;
	}

	public void setFilters(ArrayList<Filter> filters) {
		this.Filters = filters;
	}
	public void addFilters(Filter filter) {
		this.Filters.add(filter);
	}
}


