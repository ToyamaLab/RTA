package rtaclient.sjmanager;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class tableDAGNode {
    private List<tableDAGNode> previousNodes = new ArrayList<>();
    private List<tableDAGNode> followingNodes = new ArrayList<>();
    private Map<String, Integer> attributCardinality = new HashMap<>();
    private int tableCardinality;


    public Map<String, Integer> getAttributCardinality() {
        return attributCardinality;
    }

    public int getTableCardinality() {
        return tableCardinality;
    }

    public tableDAGNode(){
    }

    public tableDAGNode(List<tableDAGNode> previousNode){
        this.previousNodes=previousNode;
        for (tableDAGNode dn : previousNode){
            dn.addFollowingNode(this);
        }
    }

    public void addFollowingNode(tableDAGNode tgN){
        this.followingNodes.add(tgN);
    }

    public void setTableCardinality(int tableCardinality) {
        this.tableCardinality = tableCardinality;
    }

    public void putAttributCardinality(String attribut, int cardinality){
        attributCardinality.put(attribut,cardinality);
    }

    public void printNode(int depth){
        System.out.println("depth : " + depth + "  table_cardi : "+ tableCardinality);
        System.out.println(attributCardinality);

        for (tableDAGNode tn : followingNodes){
            tn.printNode(depth+1);
        }
    }
}
