package streamAPI

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import streaming.MyNoParallelSourceScala

object StreamingAPIPartition {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val text = env.addSource(new MyNoParallelSourceScala)
    val part = text.map(line=>{Tuple1(line)})
    val values = part.partitionCustom(new MyPartition,0)
    val unit = values.map(line => {
      println("当前线程：" + Thread.currentThread().getId + "，value：" + line)
      line._1
    })
    unit.print().setParallelism(1);
    env.execute();



  }

}
