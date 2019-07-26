package rtaclient.sjmanager;


import java.util.ArrayList;

public class attributDAGRoot {
    private attributDAGNode firstNode;
    private attributDAGNode topNode;
    private String attribut;            //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! a voir si utile


    public attributDAGNode getTopNode(){ return topNode;}

    public attributDAGNode getFirstNode(){return firstNode;}

    public attributDAGRoot(String attribut, int cardinality){
        this.attribut=attribut;
        firstNode = new attributDAGNode(cardinality);
        topNode=firstNode;
    }

    public void addSJNodeToTop(attributDAGNode diagonalNode){
        ArrayList<attributDAGNode> previousNodes = new ArrayList<>();
        previousNodes.add(this.topNode);
        previousNodes.add(diagonalNode);
        attributDAGNode dg = new attributDAGNode(previousNodes,000000000000000111111111);
        this.topNode=dg;
    }

    public void addSelectNodeToTop(int cardinality){
        attributDAGNode dg = new attributDAGNode(topNode, cardinality);
        this.topNode=dg;
    }


    public float multiplyAllEdges(ArrayList<Integer> listNodes, ArrayList<Integer> domainSize){
        return topNode.multiplyEdge(listNodes, domainSize);
    }

    public void printTree(){
        System.out.println(attribut);
        firstNode.printNode(1);
    }



}
