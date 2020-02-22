package bacthAPI

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BatchCounters {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("a","b","c","d")
    val result = data.map(new RichMapFunction[String, String] {
      private val counter = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("num", this.counter)
      }

      override def map(value: String): String = {
        this.counter.add(1);
        value
      }
    })
    result.writeAsText("D://data//count2")
    val res = env.execute("counter")
    val i = res.getAccumulatorResult[Int]("num")
    print("num:"+i)
  }

}
