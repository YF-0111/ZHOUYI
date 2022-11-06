package com.rogerguo.kafka.test.index;

import com.rogerguo.kafka.test.pointer.Pointer;
import com.rogerguo.kafka.test.consumer.Input_hdfs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

class InternalIndexEntry {

    private long startTime;

    private long endTime;

    private Node childNode; 

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Node getChildNode() {
        return childNode;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setChildNode(Node childNode) {
        this.childNode = childNode;
    }

}

class IndexEntry {
    private long startTime;

    private long endTime;

    private String vid;

    private Pointer pointer; 

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getVid(){
        return vid;
    }

    public Pointer getPointer() {
        return pointer;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public void setPointer(Pointer pointer) {
        this.pointer = pointer;
    }
}

class timeRange {
    private long sT;

    private long eT;

    public long geteT() {
        return eT;
    }

    public void seteT(long eT) {
        this.eT = eT;
    }

    public long getsT() {
        return sT;
    }

    public void setsT(long sT) {
        this.sT = sT;
    }
}

class DiskChildNodePointer {
    boolean isLeafNode;
    long offsetStart;
}

class Node {
    public int nodeId;
    boolean isLeafNode;
    private long startPos = -1; // disk
    long minT;
    long maxT;
    InternalNode parentNode;
    int parentInternalIndexEntryOrder = -2;
    IndexFirst indexFirst;

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public long getStartPos() {
        return startPos;
    }
}


class global{
    public static int nId = 0;
    public static int leafNodeSize = 4;
    public static int rootNodeId = 0;
    public static int internalNodeSize = 2;
    public static int oneIndexEntrySize = 80; // in disk
    public static int oneInternalIndexEntrySize = 45;  // in disk
    public static int leafNodeBlockSize = 320; // in disk
    public static int internalNodeBlockSize = 90;  // in disk
    public static String index_file_name = "/test_onefile/index/index.txt";
    public static Input_hdfs ih = new Input_hdfs();
    public static InternalIndexEntry createInternalIndexEntry(Node childNode){
        InternalIndexEntry itIE = new InternalIndexEntry();
        itIE.setStartTime(childNode.minT);
        itIE.setEndTime(childNode.maxT);
        itIE.setChildNode(childNode);
        return itIE;
    }
    public static HashMap<Integer,LeafNode> leafMap = new HashMap<>();
    public static HashMap<Integer, InternalNode> internalMap = new HashMap<>();
}

public class IndexFirst {
    private LeafNode node;
    private InternalNode rootNode;
    private LeafNode activeNode;
    private int currentStoredNodeNumber=0;
    public void setCurrentStoredNodeNumber(int currentStoredNodeNumber){
        this.currentStoredNodeNumber = currentStoredNodeNumber;
    }
    public int getCurrentStoredNodeNumber(){
        return this.currentStoredNodeNumber;
    }
    public void printLeafMap(){
        for(HashMap.Entry<Integer,LeafNode> entry : global.leafMap.entrySet()){
            System.out.println("_______________loop _ leaf_________________");
            printSearch(entry.getValue(), 1);
            System.out.println("_______________loop _ leaf_________________");
        }
    }
    public void printInternalMap(){
        for(HashMap.Entry<Integer,InternalNode> entry : global.internalMap.entrySet()){
            System.out.println("_______________loop _ internal_________________");
            int idE_num = 0;
            for(InternalIndexEntry idE : entry.getValue().internalIndexEntries){
                System.out.println("index Entry " + idE_num);
                idE_num ++;
                printInternalIndexEntry(idE);
            }
            System.out.println("_______________loop _ internal_________________");
        }
    }

    public IndexFirst() {
        this.node = new LeafNode();
        node.setIndexFirst(this);
        global.nId ++ ;
        this.node.nodeId = global.nId;
        setActiveNode(node);
    }

    public LeafNode getActiveNode() {
        return activeNode;
    }

    public InternalNode getRootNode() {
        return rootNode;
    }

    public int getRootNodeID() {
        return rootNode.nodeId;
    }

    public void setActiveNode(LeafNode activeNode) {
        this.activeNode = activeNode;
    }

    public void setRootNode(InternalNode rootNode) {
        this.rootNode = rootNode;
    }

    public IndexEntry convertInputToIndexEntry(String vertex_id, String t1, String t2,
                                               long offset1, long offset2){
        IndexEntry iE = new IndexEntry();
        Pointer p = new Pointer();
        iE.setStartTime(Long.parseLong(t1));
        iE.setEndTime(Long.parseLong(t2));
        iE.setVid(vertex_id);
        p.offsetStart = offset1;
        p.offsetEnd = offset2;
        iE.setPointer(p);
        return iE;
    }

    public void indexTreeGeneration(IndexEntry idE, int nLeafNode) throws Exception {
        getActiveNode().insertLeafNode(idE,nLeafNode);
    }

    
    public void writeLeafNodeIntoIndexFile(LeafNode node, String indexFile) throws Exception {
        global.ih.init_hdfs();
  
        long offset=0;
        Path index_file_path = new Path(indexFile);
        File i_file = new File(indexFile);
        FSDataOutputStream i_outputStream;
        System.out.println("index file exists? "+global.ih.isPathExist(indexFile));
        if(!global.ih.isPathExist(indexFile)){
            System.out.println("no");
            i_outputStream = global.ih.fs.create(index_file_path);
        }else{
            System.out.println("yes");
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
      
        for(IndexEntry idE : node.indexEntries){
            content = "";
            content = Long.toString(idE.getStartTime())+"," + Long.toString(idE.getEndTime()) + ","+idE.getVid() + ","
                    + Long.toString(idE.getPointer().offsetStart) + "," + Long.toString(idE.getPointer().offsetEnd)+";";
            //        content = String.format("%-60s", content);
            content = String.format("%-" + global.oneIndexEntrySize + "s", content);
            // 13+1+13+1+ 8+1+10+1+10+1=79  80

            i_outputStream.writeBytes(content);
        }
        i_outputStream.close();
        setCurrentStoredNodeNumber(getCurrentStoredNodeNumber()+1);
    }

 
    public void writeInMemoryNodeInToDisk(String rootNodeFile_name, String index_file_name) throws Exception {
        InternalNode currentParentNode = this.activeNode.parentNode;
        while(currentParentNode != null){
            if(currentParentNode.internalIndexEntries.size()<global.internalNodeSize) {
                writeIncompleteInternalNodeIntoIndexFile(currentParentNode, index_file_name);   
            }
            currentParentNode = currentParentNode.parentNode;
        } 
        writeLeafNodeIntoIndexFile(this.activeNode,rootNodeFile_name);
        writeInternalNodeIntoIndexFile(this.rootNode,rootNodeFile_name);
    }
    public void writeInternalNodeIntoIndexFile(InternalNode node, String indexFile) throws Exception {
        global.ih.init_hdfs();
        long offset=0;

        Path index_file_path = new Path(indexFile);
       
        FSDataOutputStream i_outputStream;

        if(!global.ih.isPathExist(indexFile)){
            i_outputStream = global.ih.fs.create(index_file_path);
        }else{
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
        for(InternalIndexEntry itIE : node.internalIndexEntries){
            content = "";
            content = Long.toString(itIE.getStartTime())+","+ Long.toString(itIE.getEndTime()) + ","
                    + Boolean.toString(itIE.getChildNode().isLeafNode) + "," + Long.toString(itIE.getChildNode().getStartPos()) + ";";
            content = String.format("%-" + global.oneInternalIndexEntrySize + "s", content);
            i_outputStream.writeBytes(content);
        }
        i_outputStream.close();
        if(node.parentNode != null)
            node.parentNode.internalIndexEntries.get(node.parentInternalIndexEntryOrder).getChildNode().setStartPos(offset);
    }
    public void writeIncompleteInternalNodeIntoIndexFile(InternalNode node, String indexFile) throws Exception {
        global.ih.init_hdfs();
        long offset=0;

        Path index_file_path = new Path(indexFile);
       
        FSDataOutputStream i_outputStream;

        if(!global.ih.isPathExist(indexFile)){
            i_outputStream = global.ih.fs.create(index_file_path);
        }else{
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
        for(InternalIndexEntry itIE : node.internalIndexEntries){
            content = "";
            content = Long.toString(itIE.getStartTime())+","+ Long.toString(itIE.getEndTime()) + ","
                    + Boolean.toString(itIE.getChildNode().isLeafNode) + "," + Long.toString(itIE.getChildNode().getStartPos()) + ";";
            content = String.format("%-" + global.oneInternalIndexEntrySize + "s", content);
            i_outputStream.writeBytes(content);
        }
        content = " ";
        content = String.format("%-" + (global.internalNodeSize-node.internalIndexEntries.size())*global.oneInternalIndexEntrySize + "s", content);
        i_outputStream.writeBytes(content);
       
        i_outputStream.close();
        if(node.parentNode != null)
            node.parentNode.internalIndexEntries.get(node.parentInternalIndexEntryOrder).getChildNode().setStartPos(offset);
    }
    public void search(long startTime, long endTime, List<Pointer> result,
                       String vid, String indexFile, String rootFile) throws Exception {

        Queue<Node> nodeQueue = new LinkedList<>();

        if(rootNode != null){
            if(nodeQueue.offer(rootNode)) {
                System.out.println("Success add root");
            }else{
                System.out.println("add root fail");
            }

            while(! nodeQueue.isEmpty()){
                Node node = nodeQueue.poll();
                // in disk
                if(node.getStartPos() != -1 ){
                    searchInDisk(startTime,endTime,result,vid,indexFile,node.isLeafNode,node.getStartPos());
                }

                // in memory
                if(node.isLeafNode) {
                    LeafNode leafNode = (LeafNode) node;
                    leafNodeSearchToResult(leafNode,startTime,endTime,vid,result);
                }else{
                    InternalNode inNode = (InternalNode) node;
                    for(InternalIndexEntry iniIdE : inNode.internalIndexEntries){
                        if(!(iniIdE.getStartTime() > endTime || iniIdE.getEndTime() < startTime)){
                            nodeQueue.offer(iniIdE.getChildNode());
                        }
                    }
                }
            }
        }

        if(!connectedToRootNode(activeNode)){
            leafNodeSearchToResult(activeNode, startTime, endTime, vid, result);
        }
    }

    public void searchFromRootFile(long startTime, long endTime, List<Pointer> result,
                       String vid, String indexFile, String rootFile) throws Exception {
            searchInDisk(startTime,endTime,result,vid,indexFile,
                    false,global.leafNodeBlockSize);
        Input_hdfs ih_readDisk = new Input_hdfs();
        ih_readDisk.init_hdfs();
        FSDataInputStream in = ih_readDisk.fs.open(new Path(rootFile));

        byte[] buffer = new byte[global.internalNodeBlockSize];
        in.read(global.leafNodeBlockSize, buffer, 0, global.internalNodeBlockSize);
        String interalEntryList = IOUtils.toString(buffer,"utf-8");
        String[] internalEntries = interalEntryList.split(";");
        for(String internalEntry : internalEntries){
            String[] datas = internalEntry.split(",");
            if(!(Long.parseLong(datas[0].trim()) > endTime || Long.parseLong(datas[1]) < startTime) ){
                searchInDisk(startTime,endTime,result,vid,indexFile,
                        Boolean.getBoolean(datas[2]),Long.getLong(datas[3]));
            }
        }
    }

    public boolean connectedToRootNode(LeafNode leafNode){
        InternalNode currentParentNode = leafNode.parentNode;
        while(currentParentNode != null){
            if(currentParentNode == rootNode)
                return true;
            currentParentNode = currentParentNode.parentNode;
        }
        return  false;
    }

    public void searchInDisk(long startTime, long endTime, List<Pointer> result,
                             String vid, String indexFile, boolean isLeafNode,
                             long nodeStartPos) throws Exception {

        if(isLeafNode){
            result.addAll(
                    searchLeafNodeInDisk(
                            nodeStartPos,
                            nodeStartPos+global.leafNodeBlockSize,
                            indexFile,
                            vid,
                            startTime,
                            endTime));
        }
        else{
            List<DiskChildNodePointer> dCNPs = new ArrayList<>();
            dCNPs.addAll(
                    searchInternalNodeInDisk(
                            nodeStartPos,
                            nodeStartPos+global.internalNodeBlockSize,
                            indexFile,
                            startTime,
                            endTime));
            for(DiskChildNodePointer dCNP : dCNPs){
                searchInDisk(startTime,endTime,result,vid,indexFile,dCNP.isLeafNode,dCNP.offsetStart);
            }

        }
    }

    public void leafNodeSearchToResult(LeafNode leafNode, long startTime, long endTime,
                                       String vid, List<Pointer> result){
        for(IndexEntry idE : leafNode.indexEntries){
            if(!(idE.getStartTime() > endTime || idE.getEndTime() < startTime) && vid.equals(idE.getVid())){
                result.add(idE.getPointer());
            }
        }
    }

    public List<Pointer> searchLeafNodeInDisk(long offset1, long offset2, String indexFile,
                                              String vid, long startT, long endT) throws Exception {
        Input_hdfs ih_read = new Input_hdfs();
        ih_read.init_hdfs();
        List<Pointer> pointerList = new ArrayList<>();
        Pointer pointer = new Pointer();

        FSDataInputStream in = ih_read.fs.open(new Path(indexFile));
        Long length = offset2 - offset1;

        byte[] buffer = new byte[length.intValue()];
        in.read(offset1, buffer, 0, length.intValue());
        String entryList = IOUtils.toString(buffer,"utf-8");
        String[] entries = entryList.split(";");

        for(String entry : entries){
            String[] datas = entry.split(",");
            if(!(Long.parseLong(datas[0].trim()) > endT || Long.parseLong(datas[1]) < startT) && vid.equals(datas[2])){
                pointer.offsetStart = Long.parseLong(datas[3]);
                pointer.offsetEnd = Long.parseLong(datas[4]);
                pointerList.add(pointer);
            }
        }
        return pointerList;
    }

    public List<DiskChildNodePointer> searchInternalNodeInDisk(long offset1, long offset2, String indexFile,
                                                               long startT, long endT) throws Exception {
        Input_hdfs ih_read = new Input_hdfs();
        ih_read.init_hdfs();
        List<DiskChildNodePointer> pointerList = new ArrayList<>();
        DiskChildNodePointer pointer = new DiskChildNodePointer();

        FSDataInputStream in = ih_read.fs.open(new Path(indexFile));
        Long length = offset2 - offset1;

        byte[] buffer = new byte[length.intValue()];
        in.read(offset1, buffer, 0, length.intValue());
        String entryList = IOUtils.toString(buffer,"utf-8");
        String[] entries = entryList.split(";");
       
        for(String entry : entries){
            String[] datas = entry.split(",");
            if(!(Long.parseLong(datas[0].trim()) > endT || Long.parseLong(datas[1]) < startT) ){
                pointer.isLeafNode = Boolean.getBoolean(datas[3]);
                pointer.offsetStart = Long.parseLong(datas[4]);
                pointerList.add(pointer);
            }
        }
        return pointerList;
    }

    public void printIDSearch(int nid, int deep){
        System.out.println("deep = " + deep);
        if(global.leafMap.containsKey(nid)){
            printSearch(global.leafMap.get(nid),deep);
        }else if(global.internalMap.containsKey(nid)){
            printSearch(global.internalMap.get(nid),deep);
        }else{
            System.out.println("error");
        }
    }
    public void printSearch(LeafNode n, int deep){
        int idE_num = 0;
        for(IndexEntry idE : n.indexEntries){
            System.out.println("index Entry " + idE_num);
            idE_num ++;
            printIndexEntry(idE);
        }
    }
    public void printSearch(InternalNode n, int deep){
        int itIE_num = 0;
        for(InternalIndexEntry itIE : n.internalIndexEntries){
            System.out.println("internal Index Entry " + itIE_num);
            itIE_num ++;
            printInternalIndexEntry(itIE);
            printIDSearch(itIE.getChildNode().nodeId,deep+1);
        }
    }
    public void printIndexEntry(IndexEntry idE){
        System.out.println("    startTime = " + idE.getStartTime());
        System.out.println("    endTime = " + idE.getEndTime());
        System.out.println("    vid = " + idE.getVid());
        System.out.println("    offsetStart = " + idE.getPointer().offsetStart);
        System.out.println("    offsetEnd = " + idE.getPointer().offsetEnd);
    }
    public void printInternalIndexEntry(InternalIndexEntry itIE){
        System.out.println("    startTime = " + itIE.getStartTime());
        System.out.println("    endTime = " + itIE.getEndTime());
    }


}

class LeafNode extends Node{
    // index entry
    List<IndexEntry> indexEntries;
    private IndexFirst indexFirst;
    public void setIndexFirst(IndexFirst indexFirst){
        this.indexFirst = indexFirst;
    }
    public IndexFirst getIndexFirst(){
        return this.indexFirst;
    }

    public LeafNode() {
        isLeafNode = true;
        indexEntries = new ArrayList<>();
        minT = 0;
        maxT = 0;
    }

    public void insertLeafNode(IndexEntry IndexEntry,int nLeafNode) throws Exception {

        if(indexEntries.size() >= global.leafNodeSize){
    
            
            if(indexFirst.getCurrentStoredNodeNumber()>=nLeafNode){
                System.out.println("there are n leaf node written into index file");
                return ;
            }
            
     
            LeafNode newLeafNode = new LeafNode();
            newLeafNode.setIndexFirst(indexFirst);
            global.nId ++;
            newLeafNode.nodeId = global.nId;

            newLeafNode.indexEntries.add(IndexEntry);
  
            newLeafNode.minT = IndexEntry.getStartTime();
            newLeafNode.maxT = IndexEntry.getEndTime();
   
            indexFirst.setActiveNode(newLeafNode);
            InternalIndexEntry iIE = global.createInternalIndexEntry(newLeafNode);
            global.leafMap.put(newLeafNode.nodeId,newLeafNode);

            if(this.parentNode != null){
                if(this.parentNode.internalIndexEntries.size()<global.internalNodeSize){
                    newLeafNode.parentNode = this.parentNode;
                }
                this.parentNode.insertInternalNode(iIE);
            }
            else {
                InternalNode newParentNode = new InternalNode(this.indexFirst);
           
                global.nId ++ ;
                newParentNode.nodeId = global.nId;

                newLeafNode.parentNode = newParentNode;
                this.parentNode = newParentNode;

                InternalIndexEntry oldIIE = global.createInternalIndexEntry(this);
                this.parentNode.insertInternalNode(oldIIE);
                this.parentNode.insertInternalNode(iIE);
                indexFirst.setRootNode(newParentNode);
            }
            // if(indexEntries.size() >= global.leafNodeSize)
            // indexFirst.writeLeafNodeIntoIndexFile(this,global.index_file_name);
            System.out.println("insert internal node");
        } 
        else{
            System.out.println("insert leaf node");
            indexEntries.add(IndexEntry);
           
            if(IndexEntry.getStartTime()<minT || minT == 0) {
                minT = IndexEntry.getStartTime();
            }
            if(IndexEntry.getEndTime()>maxT)
                maxT = IndexEntry.getEndTime();
            if(indexEntries.size() >= global.leafNodeSize)
                indexFirst.writeLeafNodeIntoIndexFile(this,global.index_file_name);
            if(this.parentNode != null){
                parentNode.updateInternalNode(this);
            }
            InternalNode pNode = this.parentNode;
            while(pNode!=null && pNode.internalIndexEntries.size()==global.internalNodeSize && 
            pNode.internalIndexEntries.get(global.internalNodeSize-1).getChildNode().getStartPos()!=-1){
            
                indexFirst.writeInternalNodeIntoIndexFile(pNode,global.index_file_name);
                pNode = pNode.parentNode;
            }
        }
        global.leafMap.put(this.nodeId,this);
    }
}

class InternalNode extends Node {
    List<InternalIndexEntry> internalIndexEntries;
    public InternalNode(IndexFirst indexFirst){
        this.setIndexFirst(indexFirst);
        isLeafNode = false;
        internalIndexEntries = new ArrayList<>();
        minT = 0;
        maxT = 0;
    }
    public void setIndexFirst(IndexFirst indexFirst){
        this.indexFirst = indexFirst;
    }
    public IndexFirst getIndexFirst(){
        return this.indexFirst;
    }

    public void updateInternalNode(Node n){
        System.out.println("update()");
        InternalNode thisNodesParentNode = n.parentNode;
        while(thisNodesParentNode != null){
            System.out.println("while have parent node");
       
            InternalIndexEntry iIE;
            iIE = thisNodesParentNode.internalIndexEntries.get(thisNodesParentNode.internalIndexEntries.size()-1);
   
            if(n.minT < iIE.getStartTime()){
                iIE.setStartTime(n.minT);
            }
            if(n.maxT > iIE.getEndTime()){
                iIE.setEndTime(n.maxT);
            }
            
            if(iIE.getStartTime() < thisNodesParentNode.minT || thisNodesParentNode.minT == 0) {
                System.out.println("if1");
                thisNodesParentNode.minT = iIE.getStartTime();
                global.internalMap.put(thisNodesParentNode.nodeId,thisNodesParentNode);
            }
            if(iIE.getEndTime() > thisNodesParentNode.maxT) {
                System.out.println("if2");
                thisNodesParentNode.maxT = iIE.getEndTime();
                global.internalMap.put(thisNodesParentNode.nodeId,thisNodesParentNode);
            }

            thisNodesParentNode = thisNodesParentNode.parentNode;
        }
    }

    public void insertInternalNode(InternalIndexEntry inidE) throws Exception {
        
        if(internalIndexEntries.size() >= global.internalNodeSize){

            InternalNode newInternalNode = new InternalNode(this.indexFirst);

            global.nId ++ ;
            newInternalNode.nodeId = global.nId;
            newInternalNode.internalIndexEntries.add(inidE);

            inidE.getChildNode().parentInternalIndexEntryOrder = newInternalNode.internalIndexEntries.size()-1;
            
            newInternalNode.minT = inidE.getStartTime();
            newInternalNode.maxT = inidE.getEndTime();
            inidE.getChildNode().parentNode = newInternalNode;
            InternalIndexEntry iIE = global.createInternalIndexEntry(newInternalNode);
            if(this.parentNode != null){
                if(this.parentNode.internalIndexEntries.size()<global.internalNodeSize){
                    newInternalNode.parentNode = this.parentNode;
                }
                this.parentNode.insertInternalNode(iIE);
            }else {
                InternalNode newParentNode = new InternalNode(this.indexFirst);

                global.nId ++ ;
                newParentNode.nodeId = global.nId;

                newInternalNode.parentNode = newParentNode;
                this.parentNode = newParentNode;
                InternalIndexEntry oldIIE = global.createInternalIndexEntry(this);
                newParentNode.insertInternalNode(oldIIE);
                newParentNode.insertInternalNode(iIE);
                indexFirst.setRootNode(newParentNode);
                global.internalMap.put(newParentNode.nodeId,newParentNode);
            }
            global.internalMap.put(newInternalNode.nodeId,newInternalNode);
        } else{
            internalIndexEntries.add(inidE);
            inidE.getChildNode().parentInternalIndexEntryOrder = internalIndexEntries.size()-1;
            if(inidE.getStartTime()<minT || inidE.getEndTime()>maxT || minT == 0){
                if(inidE.getStartTime()<minT || minT == 0)
                    minT = inidE.getStartTime();
                if(inidE.getEndTime()>maxT)
                    maxT = inidE.getEndTime();
                if(this.parentNode != null)
                    parentNode.updateInternalNode(this);
            }
        }
        global.internalMap.put(this.nodeId,this);
    }
}