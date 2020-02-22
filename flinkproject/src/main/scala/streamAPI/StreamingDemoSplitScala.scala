package streamAPI

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import streaming.MyNoParallelSourceScala

object StreamingDemoSplitScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text = env.addSource(new MyNoParallelSourceScala)
    val splitdata = text.split(new OutputSelector[Long] {
      override def select(out: Long) = {
        val list = new util.ArrayList[String]()
        if (out % 2 == 0) {
          list.add("odd")
        } else {
          list.add("even")
        }
        list
      }
    })
    val unit = splitdata.select("odd")
    unit.print().setParallelism(1)
    env.execute("split")





  }

}
