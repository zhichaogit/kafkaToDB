import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Properties;
import java.util.concurrent.ExecutionException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerTest{


    public static void main(String[] args){
	int records = 1;

	if (args.length > 0) {
	    records = Integer.parseInt(args[0]);
	}

        Properties props = new Properties();

        props.put("bootstrap.servers", "172.16.20.12:6667");

        props.put("retries", 3);

        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<Long, byte[]> producer = new KafkaProducer<Long, byte[]>(props);

        String topic="g_ad";

        byte[] msg1 = new byte[]{0,1,2,3,(byte)155,5,6,7,8,9,10,11,12,13,14,'a','a'};
        byte[] msg2 = new byte[]{0,1,2,3,(byte)155,5,6,7,8,9,10,11,12,13,14,'a','a','a','a','a'};
        byte[] msg3 = new byte[]{0,1,2,3,(byte)155,5,6,7,8,9,10,11,12,13,14,'a','a','a','a','a','a'};

	for (int i = 0; i < records; i++) {
	    send_msg(producer, topic, msg1);
	    send_msg(producer, topic, msg2);
	    send_msg(producer, topic, msg3);
	}

        producer.close();
    }

    public static void send_msg(KafkaProducer<Long, byte[]> producer, String topic, byte[] msg){
	ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(topic, msg);
	producer.send(record);
    }
}

