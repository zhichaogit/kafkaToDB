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

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("retries", 3);

        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<Long, byte[]> producer = new KafkaProducer<Long, byte[]>(props);

        String topic="g_ad";

        byte[] msg = new byte[]{0,1,2,3,(byte)155,5,6,7,8,9,10,11,12,13,14,15,16,17,18};


        ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(topic, msg);

        producer.send(record);

        producer.close();
    }
}

