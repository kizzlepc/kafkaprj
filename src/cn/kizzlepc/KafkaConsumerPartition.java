package cn.kizzlepc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
/**
 * 消费者API-手动提交偏移
 * @author admin
 *在某些情况下，您可能希望通过明确指定偏移量来更精确地控制已经提交的记录。
 *https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
 */

public class KafkaConsumerPartition {
	private final static String TOPIC = "test2";
	
	public static void main(String[] args) {
	    Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("group.id", "test2");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer");
        
        @SuppressWarnings("resource")
		KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        
        while(true) {
        	@SuppressWarnings("unchecked")
			ConsumerRecords<String,String> records = (ConsumerRecords)consumer.poll(Long.MAX_VALUE);
        	Set<TopicPartition> partitions = records.partitions();
        	for(TopicPartition partition:partitions) {
        		List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        		for(ConsumerRecord<String, String> record:partitionRecords) {
        			System.out.println(partition.partition()+"--"+record.offset()+"--"+record.value());
        		}
        		long lastOffset = partitionRecords.get(partitionRecords.size()-1).offset();
        		consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset+1)));
        	}
        }
	}

}
