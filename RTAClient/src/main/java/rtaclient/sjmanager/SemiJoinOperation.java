package rtaclient.sjmanager;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import rtaclient.db.DBConnect;

import rtaclient.common.GlobalEnv;
import rtaclient.parser.Parser;
import rtaclient.parser.Utils;
import rtaclient.TableConnector;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class SemiJoinOperation {

    //the left Table/Attributs will be the remote Table on which the SJ is being done
    //the right Table/Attribut will be a local/remote table on which the data is being fetched
    List<Expression> leftSJAttribut = new ArrayList<>();
    List<Expression> rightSJAttribut = new ArrayList<>();
    TableConnector rightTable;
    TableConnector leftTable;
    boolean localRightTable;

    public TableConnector getLeftTable() {
        return leftTable;
    }

    public TableConnector getRightTable() {return rightTable;}


    public static void executeAllSJ(Parser parser){
        int maxProfite = 0;
        int indexMaxSJ = 0;
        ArrayList<SemiJoinOperation> allSJ = new ArrayList<>(parser.getSJPlan());
        while(!allSJ.isEmpty()){
            maxProfite=0;
            indexMaxSJ=0;
            for (int i=0; i<allSJ.size();++i){
                int benefit = allSJ.get(i).estimateBenefitsSJ(parser).get("estimate_benefit");
                System.out.println(benefit);
                if (benefit <=allSJ.get(i).estimateCostSJ(parser)){
                    allSJ.remove(i);
                    break;
                }
                if(maxProfite < benefit-allSJ.get(i).estimateCostSJ(parser)) {
                    maxProfite = benefit - allSJ.get(i).estimateCostSJ(parser);
                    indexMaxSJ=i;
                }
                if (i==allSJ.size()-1){
                    System.out.println("DECISIOn TO DO SJ : benefit "+allSJ.get(indexMaxSJ).estimateBenefitsSJ(parser).get("estimate_benefit") + "cost : "+allSJ.get(indexMaxSJ).estimateCostSJ(parser));
                    allSJ.get(indexMaxSJ).executeSJ(parser);
                    allSJ.remove(indexMaxSJ);
                }
            }
        }
    }

    public void setRightTableAccessMethod (String method){
        rightTable.setAccessMethod(method);
    }

    public void setLeftTableName(String newName){this.leftTable.setTbName(newName); }

    public boolean sameHost(){return this.leftTable.getHost().equals(this.rightTable.getHost());}

    public boolean checkAlreadyExist(TableConnector tcLeft, TableConnector tcRight, Expression leftAttribut, Expression rightAttribut){
        if (tcLeft.equals(leftTable)&&tcRight.equals(rightTable)){
            this.leftSJAttribut.add(leftAttribut);
            this.rightSJAttribut.add(rightAttribut);
            return true;
        } else if (tcLeft.equals(rightTable)&&tcRight.equals(leftTable)){
            this.leftSJAttribut.add(rightAttribut);
            this.rightSJAttribut.add(leftAttribut);
            return true;
        } else {
            return false;
        }
    }

    public SemiJoinOperation( TableConnector leftTable, TableConnector rightTable, Expression leftSJAttribut, Expression rigthSJAttribut, boolean localRightTable){
        this.leftSJAttribut.add(leftSJAttribut);
        this.rightSJAttribut.add(rigthSJAttribut);
        this.leftTable=leftTable;
        this.rightTable=rightTable;
        this.localRightTable=localRightTable;
    }



    public void print(){
        if (rightTable == null){
            System.out.println("rightTable : null");
        }else {
            System.out.println("rightTable : "+rightTable.getTbName());
        }
        System.out.println("leftAtt : "+leftSJAttribut+" rightAtt : "+rightSJAttribut);
        System.out.println("leftTable : "+leftTable.getTbName());
        System.out.println("local : "+ localRightTable);
    }



    /**
     * all the math here come from SSD1 page 613
     * @return
     */
    public Map<String, Integer> estimateBenefitsSJ(Parser parser){
        if (leftTable.getAccessMethod().equals("direct")){
            Map<String, Integer> returnMap = new HashMap<>();
            returnMap.put("estimate_benefit",-1);
        }
        Map<String, Integer> pointerToCurrentCardinality = parser.getTableDAGRoots().get(leftTable.getAccessName()).getTopNode().getAttributCardinality();
        Map<String, Integer> estimatedAttributCardinality = new HashMap<String, Integer>(parser.getTableDAGRoots().get(leftTable.getAccessName()).getTopNode().getAttributCardinality());

        int estimateTableCardinality = parser.getTableDAGRoots().get(leftTable.getAccessName()).getFirstNode().getTableCardinality();

        int maxDomainSize;
        for (int i=0; i<leftSJAttribut.size(); ++i){
            float selectivity =1;
            String currentAttribut=splitAttributAndIndex(leftSJAttribut.get(i).toString());
            ArrayList<Integer> edgeList= new ArrayList<>();
            ArrayList<Integer> domainSize= new ArrayList<>();

            selectivity*= rightTable.getAttributDAGRoots().get(splitAttributAndIndex(rightSJAttribut.get(i).toString())).multiplyAllEdges(edgeList,domainSize);
            selectivity*= leftTable.getAttributDAGRoots().get(currentAttribut).multiplyAllEdges(edgeList,domainSize);

            //this is the resultat of eventual cut on current attribut cardinality, due to the sub-sj of other attribut
            selectivity*=estimatedAttributCardinality.get(currentAttribut)/pointerToCurrentCardinality.get(currentAttribut);

            //je sais pas trop
            maxDomainSize = maxValue(domainSize);
            for (int size : domainSize){
                selectivity*= (float) size/ (float) maxDomainSize;
            }

            int newCardiEstimation = Math.round( selectivity*maxDomainSize);

            estimateTableCardinality= Math.round( (float)  newCardiEstimation/ (float) estimatedAttributCardinality.get(currentAttribut) * (float) estimateTableCardinality);

            System.out.println(estimateTableCardinality);
            System.out.println(estimatedAttributCardinality);

            for (Map.Entry iterator : estimatedAttributCardinality.entrySet()){
                if(iterator.getKey().equals(currentAttribut)){
                    estimatedAttributCardinality.put(iterator.getKey().toString(),newCardiEstimation);
                } else {
                    estimatedAttributCardinality.put(iterator.getKey().toString(),Y(estimateTableCardinality,(int) iterator.getValue()));
                }
            }



        }
        estimatedAttributCardinality.put("estimate_table_cardinality",estimateTableCardinality);
        estimatedAttributCardinality.put("estimate_benefit",parser.getTableDAGRoots().get(leftTable.getAccessName()).getFirstNode().getTableCardinality() - estimateTableCardinality);
        return estimatedAttributCardinality;
    }

    public int estimateCostSJ(Parser parser){
        /*if (leftTable.getHost().equals(rightTable.getHost())){
            this.setRightTableAccessMethod("on_site");
            return  0;
        }*/

        return rightTable.getAttributDAGRoots().get(splitAttributAndIndex(rightSJAttribut.get(0).toString())).getFirstNode().getCardinality();
    }

    private static int maxValue(ArrayList<Integer> listDomain) {
        int max = listDomain.get(0);
        for (int i = 0; i < listDomain.size(); i++) {
            if (listDomain.get(i) > max) {
                max = listDomain.get(i);
            }
        }
        return max;
    }

    /**
     * Math function from SDD1 research paper
     * @param n new value of c(R)
     * @param b old value of c(R.att)
     * @return estimation of new c(R.att) after a SJ on an other attribut R.att'
     */
    public int Y(int n,int b){
        if (n<Math.round(b/2)) return n;
        else if (n<2*b) return Math.round((float)(n+b)/3);
        else return b;
    }


    /**
     *
     * @return the name of the SJ result at the remote DB
     */
    public String executeSJ(Parser parser){


        //add nodes to the DAGtrees
        for (Map.Entry current : leftTable.getAttributDAGRoots().entrySet()){
            attributDAGRoot an = (attributDAGRoot) current.getValue();
            boolean nodeCreated= false;
            for (int i =0; i< leftSJAttribut.size(); ++i){
                if(current.getKey().equals(splitAttributAndIndex(leftSJAttribut.get(i).toString()))){
                    System.out.println(splitAttributAndIndex(rightSJAttribut.get(i).toString()));
                    System.out.println(rightTable.getAttributDAGRoots());
                    rightTable.getAttributDAGRoots().get(splitAttributAndIndex(rightSJAttribut.get(i).toString())).getTopNode().printNode(1);

                    an.addSJNodeToTop(rightTable.getAttributDAGRoots().get(splitAttributAndIndex(rightSJAttribut.get(i).toString())).getTopNode());
                    nodeCreated=true;
                    break;
                }
            }
            if(!nodeCreated){
                an.addSelectNodeToTop(0);
            }
        }

        List<SelectItem> selectItems = new ArrayList<>();
        for ( Expression att : rightSJAttribut) {
            SelectExpressionItem item = new SelectExpressionItem((Column) att);
            selectItems.add(item);
        }
        Select tmpSelect = Utils.buildSelect(rightTable.getFromItems(), selectItems ,rightTable.getWhere() );


        System.out.println("AAGGVAYUDTADCAY" + tmpSelect);

        String post_data="";

        // Set the request depending the right Table location
        if (localRightTable) {
            try {
                post_data=getDataFromLocalDB();

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e){
                e.printStackTrace();
            }

        } else {
            post_data="{\"not_on_site_table_name\":\""+ rightTable.getTbName() +"\",\"not_on_site_join_attributs\":[";
            for( Expression e : rightSJAttribut){
                post_data += "{\"att\":\""+splitAttributAndIndex(e.toString())+"\"},";
            }
            post_data=post_data.substring(0,post_data.length()-1)+"],\"on_site_table_name\":\""+ leftTable.getTbName() +"\",\"on_site_join_attributs\":[";
            for( Expression e : leftSJAttribut){
                post_data += "{\"att\":\""+splitAttributAndIndex(e.toString())+"\"},";
            }
            post_data=post_data.substring(0,post_data.length()-1)+"],\"on_site_all_attributs\":[";
            for( SelectItem si : leftTable.getSelectItem()){
                post_data += "{\"att\":\""+splitAttributAndIndex(si.toString())+"\"},";
            }


            switch (rightTable.getAccessMethod()) {
                case "API":
                    post_data= post_data.substring(0,post_data.length()-1)+"],\"accessMethode\":\"API\",\"host\":\""+rightTable.getHost()+"\"}";
                    break;
                case "direct":
                    post_data= post_data.substring(0,post_data.length()-1)+"],\"accessMethode\":\"direct\",\"dbms\":\""+rightTable.getDBMS()+"\",\"user\":\""+rightTable.getUser()+"\",\"password\":\""+rightTable.getPassword()+"\",\"connector\":\""+rightTable.createConnector()+"\",\"sqlQuery\":\""+tmpSelect.toString()+"\"}";
                case "on_site":
                    post_data= post_data.substring(0,post_data.length()-1)+"],\"accessMethode\":\"on_site\"}";
            }
        }



        //send the query
        BufferedReader bufferedReader = null;
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        String url = leftTable.getHost()+"/doSemiJoin";

        System.out.println(url);

        HttpURLConnection httpURLConnection = null;
        try {

            httpURLConnection = (HttpURLConnection) new URL(url).openConnection();
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setRequestProperty("Content-Type", "text/plain; utf-8");
            httpURLConnection.setRequestProperty("Accept", "text/plain");
            try(OutputStream osOutputStream = httpURLConnection.getOutputStream()) {
                osOutputStream.write(post_data.getBytes(), 0, post_data.getBytes().length);
            }


            bufferedReader = new BufferedReader(
                    new InputStreamReader(
                            httpURLConnection.getInputStream(), "UTF-8"));

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }

//			System.out.println(stringBuilder.toString());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonRoot = null;
            try {
                jsonRoot = mapper.readTree(stringBuilder.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            // retrieve and treat the stats on the updates table
            parser.getTableDAGRoots().get(leftTable.getAccessName()).getTopNode().setTableCardinality(jsonRoot.get("cardinality_tbl").asInt());


            for (JsonNode node : jsonRoot.get("cardinality_att")) {
                Iterator<Map.Entry<String, JsonNode>> nodeFields = node.fields();
                String att = "";
                int cardi=0;
                while (nodeFields.hasNext()) {
                    Map.Entry<String, JsonNode> nodeField = nodeFields.next();
                    JsonNode jn = nodeField.getValue();
                    if (jn.isTextual()) {
                        att=nodeField.getValue().asText();
                    } else if (jn.isInt()) {
                        cardi=nodeField.getValue().asInt();
                    }
                }
                leftTable.getAttributDAGRoots().get(att).getTopNode().setCardinality(cardi);
            }




            return jsonRoot.get("table").asText();


        } catch (MalformedURLException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
            return stringBuilder.toString();
        } catch (IOException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
            return stringBuilder.toString();
        }




    }



    public String getDataFromLocalDB() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        String u = "";
        java.util.Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmss");
        String tmpdate = sdf.format(date);

        switch (GlobalEnv.getDriver()) {
            case "mysql":
                Class.forName("com.mysql.jdbc.Driver");
                conn = DBConnect.connectLocalTmp();
                break;
            case "postgresql":
                Class.forName("org.postgresql.Driver");
                conn = DBConnect.connectLocal();
                break;

            //did not implement the case sqlite
            case "sqlite":
                conn = DBConnect.connectLocalTmp();
                Statement s = conn.createStatement();
                s.execute("ATTACH \"" + GlobalEnv.getDb() + "\" as local");
            default:
                break;
        }


        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(rightTable.getSQL());
            String name, value, s;
            ResultSetMetaData metadata;

            ArrayList<String> q_result;
            ArrayList<String> q_results = new ArrayList<String>();
            metadata = rs.getMetaData();
            int c = metadata.getColumnCount() + 1;

            ArrayList<String> type = new ArrayList<String>();

            for (int i = 1; i < c; i++) {
                switch (metadata.getColumnType(i)) {
                    case 12: //VARCHAR
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"varchar\"");
                        break;
                    case 4: //INTEGER
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"int\"");
                        break;
                    case 2: //NUMERIC
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"numeric\"");
                        break;
                    case -5: //BIGINT
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"bigint\"");
                        break;
                    case 91: //DATE
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"date\"");
                        break;
                    case 3: //DECIMAL
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"decimal\"");
                        break;
                    case 5: //SMALLINT
                        type.add("\"" + metadata.getColumnName(i) + "\":" + "\"smallint\"");
                        break;
                }
            }

            String ty = type.toString();
            ty = "{" + ty.substring(1, ty.length() - 1) + "}";

            while (rs.next()) {
                q_result = new ArrayList<String>();
                for (int i = 1; i < c; i++) {
                    name = null;
                    value = null;
                    s = null;
                    switch (metadata.getColumnType(i)) {
                        case 12: //String
                            name = metadata.getColumnName(i);
                            value = rs.getString(i);
                            s = "\"" + name + "\":" + "\"" + value + "\"";
                            q_result.add(s);
                            break;
                        case 2: //numeric
                            name = metadata.getColumnName(i);
                            value = rs.getString(i);
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 4: //int
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case -5: //bigint
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 91: //date
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getDate(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 3: //DECIMAL
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                        case 5: //SMALLINT
                            name = metadata.getColumnName(i);
                            value = String.valueOf(rs.getInt(i));
                            s = "\"" + name + "\":" + value;
                            q_result.add(s);
                            break;
                    }
                }
                String t = q_result.toString();
                t = "{" + t.substring(1, t.length() - 1) + "}";
                q_results.add(t);
            }

            u = q_results.toString();
            String v = "{\"not_on_site_table_name\":\""+ metadata.getTableName(1)+"_"+tmpdate + "\",\"on_site_access_name\":\""+ leftTable.getTbName() +"\",\"on_site_join_attributs\":[" ;
            for( Expression e : leftSJAttribut){
                v += "{\"att\":\""+splitAttributAndIndex(e.toString())+"\"},";

            }
            v=v.substring(0,v.length()-1)+"],\"on_site_all_attributs\":[";
            for( SelectItem si : leftTable.getSelectItem()){
                v += "{\"att\":\""+splitAttributAndIndex(si.toString())+"\"},";
            }

            u= v.substring(0,v.length()-1)+"],\"accessMethode\":\"local\","+"\"metadata\":"+ty+",\"data\":[" + u.substring(1,u.length()-1)+"]}";


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return u;
    }


    public String splitAttributAndIndex(String s){
        for (int i=0; i<s.length();++i){
            if (s.charAt(i)=='.'){
                return s.substring(i+1);
            }
        }
        return s;
    }
}

