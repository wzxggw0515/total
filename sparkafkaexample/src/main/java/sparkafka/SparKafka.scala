package sparkafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
object SparKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Kafka example").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("error");
    //定以配置參數
    val kafkaparams = Map[String,Object](
    "bootstrap.servers"->"192.168.245.133:9092",
            "key.deserializer"->classOf[StringDeserializer],
            "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"spark-streaming-group",
      "auto.offset.reset"->"latest",
      "enable.auto.commit"->(true:java.lang.Boolean)
    )
    // 创建DStream，从Kafka中读取相应的消息
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent
      , Subscribe[String, String](Array("wordcount-topic"), kafkaparams))

    stream.map(x=>x.value()).flatMap(x=>x.split(",")).map(x=>(x,1))
      .reduceByKey((p1,p2)=>p1+p2).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
