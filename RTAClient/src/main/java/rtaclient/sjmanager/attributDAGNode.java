package rtaclient.sjmanager;




import java.util.ArrayList;
import java.util.List;

public class attributDAGNode {
    private static int counter=0;
    private List<attributDAGNode> previousNodes = new ArrayList<>();
    private List<attributDAGNode> followingNodes = new ArrayList<>();
    private int nodeId;
    private float selectCoeff;
    private int cardinality;


    public attributDAGNode(List<attributDAGNode> previousNodes, int newCardinality){
        nodeId=counter;
        ++counter;
        this.previousNodes=previousNodes;
        for (attributDAGNode dn : previousNodes){
            dn.addFollowingNode(this);
        }
        this.selectCoeff=1;
    }

    public attributDAGNode(attributDAGNode previousNode, int newCardinality){
        nodeId=counter;
        ++counter;
        this.previousNodes.add(previousNode);
        previousNodes.get(0).addFollowingNode(this);
        System.out.println("previous cardi "+previousNodes.get(0).getCardinality());
        System.out.println("next cardi"+ newCardinality);
        setCardinality(newCardinality);
    }

    public attributDAGNode(int cardinality){
        nodeId=counter;
        ++counter;
        this.selectCoeff=1;
        this.cardinality=cardinality;
    }

    public int getCardinality() {return cardinality;}

    public void setCardinality(int cardinality){
        this.cardinality=cardinality;
        if (previousNodes.size()>1){
            this.selectCoeff=1;
        } else{
            this.selectCoeff=(float) cardinality/ (float) previousNodes.get(0).getCardinality();
        }
    }

    public void addFollowingNode(attributDAGNode dN){
        this.followingNodes.add(dN);
    }

    public float multiplyEdge(ArrayList<Integer> listNodes, ArrayList<Integer> domainSize) {
        if (listNodes.contains(this.nodeId)){
            return 1;
        } else if (previousNodes.isEmpty()) {
            domainSize.add(cardinality);
        }
        listNodes.add(this.nodeId);
        float res = selectCoeff;
        System.out.println("nodeId "+nodeId);
        for (attributDAGNode an : previousNodes){
            res *= an.multiplyEdge(listNodes, domainSize);
        }
        return res;
    }

    public void printNode(int depth){
        System.out.println("depth : " + depth + " id: "+ nodeId);

        for (attributDAGNode tn : followingNodes){
            tn.printNode(depth+1);
        }
    }
}