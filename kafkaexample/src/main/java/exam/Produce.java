package exam;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Produce {
    public static void main(String[] args) {
        Properties propr = new Properties();
        propr.put("bootstrap.servers","192.168.245.133:9092");
        propr.put("enable.auto.commit","true");
        propr.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        propr.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propr);
        Random random = new Random();
        for(int i=0;i<10;i++){
            String key = String.valueOf(i);
            String value = "hello"+random.nextInt(100);
            producer.send(new ProducerRecord<String, String>("exam",key,value));
            producer.flush();
        }
        producer.close();
    }
}
