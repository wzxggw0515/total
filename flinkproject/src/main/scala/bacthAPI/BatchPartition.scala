package bacthAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchPartition {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data = ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")

    val text = env.fromCollection(data)

    text.mapPartition(it=>{
      //创建数据库连接，建议吧这块代码放到try-catch代码块中
      val res = ListBuffer[String]()
     while (it.hasNext) {
       val str = it.next()
       val words = str.split("\\s+")
       for(word <- words){
         res.append(word)
       }
     }
      res
      //关闭连接
    }).print()
  }

}
