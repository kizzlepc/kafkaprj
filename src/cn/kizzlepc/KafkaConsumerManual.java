package cn.kizzlepc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 消费者API-手动提交偏移
 * @author admin
 *当消息需要经过一些特殊逻辑进行处理时，手动提交就非常有必要，没有经过处理的消息不应该当成已消费。
 *enable.auto.commit设置为false，这是因为这个值默认情况下是true，只有手动设置为false后才能进行手动提交。
 *https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
 */
public class KafkaConsumerManual {
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
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        final int minBatchSize = 50;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while(true) {
        	ConsumerRecords<String, String> records = consumer.poll(100);
        	for(ConsumerRecord<String, String> record:records) {
        		System.out.println(record.offset()+"---"+record.key()+"---"+record.value());
        		buffer.add(record);
        	}
        	//System.out.println(buffer.size());
       		if(buffer.size()>=minBatchSize) {
    			//逻辑处理代码
       			System.out.println("-------------提交");
    			consumer.commitSync();
    			buffer.clear();
    		}
        }
	}

}
