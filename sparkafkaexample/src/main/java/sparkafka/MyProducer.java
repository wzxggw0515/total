package sparkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class MyProducer {
    public static void main(String[] args) throws FileNotFoundException {
        Properties propr = new Properties();
        propr.put("bootstrap.servers","192.168.245.133:9092");
        propr.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        propr.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propr);
        Scanner sc = new Scanner(new File("C:\\Users\\Administrator\\Desktop\\file.txt"));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                ProducerRecord<String, String> recod = new ProducerRecord<>("wordcount-topic", sc.nextLine());
                producer.send(recod);
                System.out.println(recod.topic()+"-->"+recod.value());
            }
        },1000,1000);
    }
}
