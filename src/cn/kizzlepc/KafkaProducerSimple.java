package cn.kizzlepc;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
/**
 * 生存者API
 * @author admin
 *Kafka客户端用于向 Kafka 集群发布记录。生产者是线程安全的，跨线程共享一个生产者实例通常比拥有多个实例要快。
 *生产者包括一个缓冲区池，它保存尚未发送到服务器的记录，以及一个后台I/O线程，负责将这些记录转换为请求并将其传输到集群。使用后未能关闭生产者将泄漏这些资源。
 *acks配置其请求被视为完整性的标准。"all"意味着领导者将等待完整的同步副本来确认记录。只要至少有一个同步复制品仍然存在，这将保证记录不会丢失。这是最强大的保证。这相当于设置acks = -1。
 *如果请求失败，生产者可以自动重试，但是由于我们指定retries 为0，所以不会重试。
 *生产者维护每个分区的未发送出去的缓冲区。这些缓冲区的大小由batch.size指定。使此更大可以缓存更多，但需要更多的内存（因为我们通常会为每个活动分区提供缓冲区）。
 *默认情况下，即使缓冲区中存在额外的未使用空间，缓冲区也可立即发送。但是，如果要减少请求数可以设置linger.ms为大于0 的毫秒数。这将指示生产者在发送请求之前等待该毫秒数，这样将有更多记录到达缓冲区。这类似于Nagle在TCP中的算法。
 *buffer.memory控制生产者可用于缓冲的总内存量。如果记录的发送速度比可以传输到服务器的速度快，那么这个缓冲空间就会耗尽。当缓冲区空间耗尽时，附加的发送呼叫将被阻塞。max.block.ms决定阻塞时间的阈值，超出此时间时，会引发TimeoutException。
 *key.serializer和value.serializer指导如何将用户提供的ProducerRecord的键和值转换成字节。您可以使用提供的ByteArraySerializer或 StringSerializer用于简单的字符串或字节类型。
 *send()方法是异步的。当被调用时，它将记录添加到待处理记录发送的缓冲区并立即返回。这允许生产者将各个记录收集在一起以获得效率。
 *https://kafka.apache.org/0102/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 */
public class KafkaProducerSimple {
	private final static String TOPIC = "test2";
	public static void main(String[] args) {
		Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                  "org.apache.kafka.common.serialization.StringSerializer");
		
		@SuppressWarnings("resource")
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		/*
		for(int i=900;i<100000000;i++) {
			producer.send(new ProducerRecord<String, String>(TOPIC, 
					Integer.toString(i), Integer.toString(i)));
		}
		producer.close();
		*/
		int messageNo = 1;
		Random rand = new Random();
		while(true) {
			String messageStr = "message_b_"+Integer.toString(messageNo);
			producer.send(new ProducerRecord<String, String>(TOPIC, messageStr));
			messageNo ++;
			
			Utils.sleep(rand.nextInt(3000));
		}
		
	}

}
