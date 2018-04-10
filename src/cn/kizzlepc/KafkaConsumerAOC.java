package cn.kizzlepc;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 消费者API-自动提交偏移
 * @author admin
 *自动提交，设定了每1000ms提交一次偏移（就是当前已读取消息的位置）。
 *首先通过bootstrap.servers设置要连接的Broker，多个可以使用逗号隔开。
 *通过group.id设置了当前的分组id，同一个分组id中的多个消费者可以通过负载均衡处理消息（消费者数量多于主题的分区数时，多出来的消费者不会被分配任何消息）。
 *通过设置enable.auto.commit为true开启自动提交。
 *自动提交的频率由 auto.commit.interval.ms 设置。
 *两个 deserializer 用于序列化 key 和 value。
 *通过 consumer.subscribe 定义了主题 TOPIC，一个消费者可以订阅多个主题。
 *通过consumer.poll获取消息，参数1000（毫秒）的含义是，当缓冲区中没有可用消息时，以此时间进行轮训等待。当设置为0时，理解返回当前可用的消息或者返回空。
 *https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
 */
public class KafkaConsumerAOC {
	private final static String TOPIC = "test2";
	
	public static void main(String[] args) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("group.id", "test2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                  "org.apache.kafka.common.serialization.StringDeserializer");
        
        @SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        while(true) {
        	ConsumerRecords<String, String> records = consumer.poll(1000);
        	for(ConsumerRecord<String, String> record:records) {
        		System.out.println(record.offset()+"---"+record.key()+"---"+record.value());
        	}
        }
	}
}
