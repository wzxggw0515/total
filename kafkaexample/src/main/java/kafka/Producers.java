package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producers {
    public static void main(String[] args) {
        Properties prope = new Properties();
        prope.put("bootstrap.servers","192.168.245.133:9092");
        prope.put("acks","1");
        prope.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prope.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> str = new KafkaProducer<String, String>(prope);
        Random random = new Random();
        for (int i=0;i<=10;i++){
            String key = String.valueOf(i);
            String data = "hello "+random.nextInt();
            str.send(new ProducerRecord<String, String>("topc",key,data));
        }
        str.close();
    }
}
