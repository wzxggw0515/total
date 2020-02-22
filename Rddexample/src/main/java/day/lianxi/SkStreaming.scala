package day.lianxi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SkStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming ").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines = ssc.socketTextStream("localhost",9999)
    val wrcounts = lines.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_)
    wrcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
