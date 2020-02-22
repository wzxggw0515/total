package bacthAPI

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object BatchBroad {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //1: 准备需要广播的数据
    val broadData = ListBuffer[Tuple2[String,Int]]()
    broadData.append(("zs",18))
    broadData.append(("ls",20))
    broadData.append(("ww",17))

    val text = env.fromCollection(broadData)
    val tomap = text.map(tup=>{Map(tup._1->tup._2)})


    val ttx = env.fromElements("zs","ls","ww")
    val bod = ttx.map(new RichMapFunction[String, String] {
      var listData: util.List[Map[String, Int]] = null
      var allMap = Map[String, Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String,Int]]("broad")
        val it = listData.iterator()
        while (it.hasNext){
          val next = it.next()
          allMap = allMap.++(next)
        }
      }

      override def map(value: String): String = {
        val age = allMap.get(value).get
        value + "," + age
      }

    }).withBroadcastSet(tomap, "broad")

    bod.print()

  }

}
