import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerTest {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "g1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<Long, byte[]> kafkaConsumer = new KafkaConsumer<Long, byte[]>(props);
	TopicPartition partition = new TopicPartition("g_ad", 0);
	kafkaConsumer.assign(Arrays.asList(partition));

	System.out.println("nothing available...");
	ConsumerRecords<Long, byte[]> records = kafkaConsumer.poll(1000);
	System.out.println("nothing available end: " + records);
	for(ConsumerRecord<Long, byte[]> record : records){
	    byte[] msg = record.value();
	    System.out.printf("key = %d, offset = %d, value = %s, length:%d\n", 
			      record.key(), record.offset(), msg, msg.length);
	}
    }
}

