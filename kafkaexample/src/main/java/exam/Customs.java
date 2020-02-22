package exam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import scala.reflect.internal.transform.UnCurry;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Customs {
    public static void main(String[] args) {
        Properties propr = new Properties();
        propr.put("bootstrap.servers","192.168.245.133:9092");
        propr.put("group.id","ex");
        propr.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        propr.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> csutom = new KafkaConsumer<String, String>(propr);
        csutom.subscribe(Arrays.asList("exam"));
        while(true){
            ConsumerRecords<String, String> records = csutom.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                System.out.println();
            }
        }

    }
}
