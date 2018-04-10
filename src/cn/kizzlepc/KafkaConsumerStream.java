package cn.kizzlepc;

import java.util.HashMap;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import kafka.consumer.KafkaStream;

/**
 * 消费者API-流
 * @author admin
 * 流API允许对来自一个或多个主题的消息进行连续计算，并将结果发送到零个，一个或多个主题中。
 * StreamsConfig.APPLICATION_ID_CONFIG用于设置当前流处理的ID，具有相同流ID的应用会根据输入主题的分区来分配任务。
 * 当流处理应用的数量大于主题的分区数时，超出部分的流处理不会被分配任何消息。
 *https://kafka.apache.org/0102/javadoc/org/apache/kafka/streams/KafkaStreams.html
 */
public class KafkaConsumerStream {
	private final static String TOPIC1 = "test2";
	private final static String TOPIC2 = "first";
 	
	public static void main(String[] args) {
		HashMap<Object, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		StreamsConfig conf = new StreamsConfig(props);
		
		KStreamBuilder builder = new KStreamBuilder();
		builder.stream(TOPIC1).mapValues(value ->{
			//Integer i = Integer.parseInt((String)value);
			Integer i = value.toString().length();
			return "stream--"+Integer.toString(i)+"--"+value;	
		}).to(TOPIC2);
		
		KafkaStreams streams = new KafkaStreams(builder, conf);
		streams.start();
	}

}
