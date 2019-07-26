package rtaclient.sjmanager;

import rtaclient.parser.Parser;

import java.util.ArrayList;
import java.util.Map;

public class tableDAGRoot {
    private ArrayList<attributDAGRoot> attributs = new ArrayList<>();
    private tableDAGNode topNode;
    private tableDAGNode firstNode;
    private String tableName;

    public tableDAGNode getFirstNode(){
        return firstNode;
    }

    public tableDAGNode getTopNode() {return topNode;}

    public tableDAGRoot(String tableName){
        this.tableName=tableName;
        firstNode = new tableDAGNode();
        topNode=firstNode;
    }

    public void addSJNodeToTop(tableDAGNode diagonalNode){
        ArrayList<tableDAGNode> previousNodes = new ArrayList<>();
        previousNodes.add(this.topNode);
        previousNodes.add(diagonalNode);
        tableDAGNode dg = new tableDAGNode(previousNodes);
        this.topNode=dg;
    }

    public void addSelectNodeToTop(){
        ArrayList<tableDAGNode> previousNodes = new ArrayList<>();
        previousNodes.add(this.topNode);
        tableDAGNode dg = new tableDAGNode(previousNodes);
        this.topNode=dg;
    }


    public void printTree(){
        firstNode.printNode(1);
    }



}
