# KafkaToHDFS
## data set
Real data set （Guarantee the system can work in the real world): 
New York Taxi Trajectory Data

Generated data set: 
Data Generator:
Network-based Generator of Moving Objects.   
https://iapg.jade-hs.de/personen/brinkhoff/generator/.  

Data Format(one line):  
*@param     action      the action of the object  
*@param     idobject    id  
*@param     repNum      report number  
*@param     objClass    object class  
*@param     time        time stamp  
*@param     x           current x-coordinate  
*@param     y           current y-coordinate  
*@param     speed       current speed  
*@param     nextNodeX   x-coordinate of the next node  
*@param     nextNodeY   y-coordinate of the next node  

## key content:

## consumer
1. Program entry: KafkaToHDFS/kafka-hbase/src/main/java/com/rogerguo/kafka/test/consumer/ConsumerTest.java
2. autoCommitOffset(): get input streaming data from kafka and build the index tree
3. createConsumer(): create a kafka consumer

## index
1. KafkaToHDFS/kafka-hbase/src/main/java/com/rogerguo/kafka/test/index/IndexFirst.java
2. Index Structure
3. class InternalIndexEntry: structure of one entry in the internal node
4. class IndexEntry: structure of one entry in the leaf node
5. class timeRange
6. class DiskChildNodePointer: address of child node in the disk
7. public class IndexFirst: related functions of tree building and searching
8. class global: global parameters
9. class Node: node structure of a tree
10. class LeafNode extends Node
11. class InternalNode extends Node

## cache
