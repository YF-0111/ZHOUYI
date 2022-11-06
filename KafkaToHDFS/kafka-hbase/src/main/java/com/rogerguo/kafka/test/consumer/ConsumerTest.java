package com.rogerguo.kafka.test.consumer;

import com.rogerguo.kafka.test.cache.Cache;
import com.rogerguo.kafka.test.index.IndexFirst;
import com.rogerguo.kafka.test.pointer.Pointer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ConsumerTest {
    public static Cache stream_data_cache = new Cache();
    public static Input_hdfs ih = new Input_hdfs();
    public static IndexFirst iF = new IndexFirst();
    public static int map_list_limit = 10;
    public static int nLeafNode = 10;
    // public static int currentStoredNodeNumber = 0;
    public static String index_file_name = "/test_onefile/index/index.txt";
    public static String rootNodeFile_name = "/test_onefile/index/rootDataFile.txt";

    public static void main(String[] args) throws Exception {

        autoCommitOffset();
        // 1,1631340658487,-82.369878,-50.97252;

        // List<Pointer> resultlist = new ArrayList<>();
        // long t1 = 163134065848L;
        // long t2 = 163134065849L;
        // String vid = "1";
        // Date day = new Date();
        // long timestamp = day.getTime();
        // // read rootNode into memory

        // iF.search(t1,t2,resultlist,vid,index_file_name,rootNodeFile_name);
        // //  1,1631340658487,-82.369878,-50.97252;
        // System.out.println(Arrays.toString(resultlist.toArray()));
    }

 
    public static Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        // 指定Kafka服务的ip地址及端口
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 指定group.id，Kafka中的消费者需要在消费者组里
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 是否开启自动提交
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交的间隔，单位毫秒
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 消息key的序列化器
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 消息value的序列化器
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("max.poll.records",40);
        return new KafkaConsumer<>(props);
    }

  
    public static void autoCommitOffset() throws Exception {
        Consumer<String, String> consumer = createConsumer();
        List<String> topics = Arrays.asList("test");
        // 订阅一个或多个Topic
        consumer.subscribe(topics);
        int count = 0;

        Set<TopicPartition> assignment = new HashSet<>();
        // 在poll()方法内部执行分区分配逻辑，该循环确保 分区已被分配。
        // 当分区消息为0时进入此循环，如果不为0，则说明已经成功分配到了分区。
        while (assignment.size() == 0) {
            consumer.poll(100);
            // assignment()方法是用来获取消费者所分配到的分区消息的
            // assignment的值为：topic-demo-3, topic-demo-0, topic-demo-2, topic-demo-1
            assignment = consumer.assignment();
        }

        Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(assignment);
        for (TopicPartition tp : assignment) {
            Long offset = beginOffsets.get(tp);
            System.out.println("分区 " + tp + " 从 " + offset + " 开始消费");
            consumer.seek(tp, offset);
        }
       
        while (iF.getCurrentStoredNodeNumber()<nLeafNode) {
            count++;
            // 从Topic中拉取数据，每1000毫秒拉取一次
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // 每次拉取可能都是一组数据，需要遍历出来

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("value = %s%n", record.value());
                // 得到数据以后分解出来
                String regex;
                String vehicle_id = "";
                String date = "";
                String latitude = "";
                String longitude = "";

                regex = "\"vehicle_id\":\"([0-9]*)\",";
                vehicle_id = string_segmentation(record.value(), regex);

                regex = "\"date\":(.*?),";
                date = string_segmentation(record.value(), regex);

                regex = "\"latitude\":(.*?),";
                latitude = string_segmentation(record.value(), regex);

                regex = "\"longitude\":(.*?)}";
                longitude = string_segmentation(record.value(), regex);

                String trajectory = "";
                trajectory = date + "," + latitude + "," + longitude + ";";
                // 写入缓存
                stream_data_cache.add_one_map(trajectory, vehicle_id);
                // 看看满了没有, 满了就进去写indexfile并且删掉缓存

                System.out.println("size=" + stream_data_cache.getStreamMap().get(vehicle_id).size());
                if (stream_data_cache.getStreamMap().get(vehicle_id).size() >= map_list_limit) {

                    // 写到hdfs
                    ih.init_hdfs();

                    // 普通file
                    String vehicle_file_name = "/test_onefile/vehicle/" + "vehicle" + ".txt";
                    System.out.println("vehicle_id:" + vehicle_id);

                    // 普通file写入
                    Path vehicle_file_path = new Path(vehicle_file_name);
                    File file = new File(vehicle_file_name);
                    FSDataOutputStream outputStream;
                    // append不支持异步
                    if (!ih.isPathExist(vehicle_file_name)) {
                        System.out.println("do not exist, create file");
                        outputStream = ih.fs.create(vehicle_file_path);
                    } else {
                        outputStream = ih.fs.append(vehicle_file_path);
                    }
                    // offset
                    long offset1 = 0, offset2 = 0;
                    offset1 = ih.get_file_length(vehicle_file_name);

                    // 写文件
                    for (String one_trajectory : stream_data_cache.getStreamMap().get(vehicle_id)) {
                        System.out.println("write " + vehicle_id + "," + one_trajectory);
                        one_trajectory = String.format("%-49s", vehicle_id + "," + one_trajectory);
                        outputStream.writeBytes(one_trajectory);
                    }
                    String index = "", t1 = "", t2 = "";

                    int size = stream_data_cache.getStreamMap().get(vehicle_id).size();
                    if (stream_data_cache.getStreamMap().get(vehicle_id) != null) {
                        t1 = stream_data_cache.getStreamMap().get(vehicle_id).get(0);
                        t2 = stream_data_cache.getStreamMap().get(vehicle_id).get(size - 1);
                        regex = "(.*?),";
                        t1 = string_segmentation(t1, regex);
                        t2 = string_segmentation(t2, regex);
                    }
                    outputStream.close();
                    // 写完后offset
                    offset2 = ih.get_file_length(vehicle_file_name);

                    index = vehicle_id + "," + t1 + "," + t2 + "," + offset1 + "," + offset2 + ";";
                    System.out.println("index" + index);
                    // Generate a Tree
                    iF.indexTreeGeneration(iF.convertInputToIndexEntry(vehicle_id, t1, t2, offset1, offset2),nLeafNode);
                    System.out.println("CurrentStoredNodeNumber: " + iF.getCurrentStoredNodeNumber());
                    
                    // 清空，直接删除key
                    stream_data_cache.clear_map_list(vehicle_id);
                }
            }
            // if(iF.getCurrentStoredNodeNumber()>=nLeafNode-1)
            //     break;
            System.out.println("count: " + count);
        }
        iF.writeInMemoryNodeInToDisk(rootNodeFile_name,index_file_name);
    }

    public static String string_segmentation(String s, String regex) {
        String result = "";
        // 创建 Pattern 对象
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(s);
        if (m.find()) {
            result = m.group(1);
        }
        return result;
    }
}
