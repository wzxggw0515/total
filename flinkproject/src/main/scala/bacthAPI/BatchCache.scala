package bacthAPI

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchCache {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    env.registerCachedFile("d:\\data\\file\\hello.txt","b.txt")

    val data = env.fromElements("hello","b","c","d")
    val unit = data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext.getDistributedCache.getFile("b.txt")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext) {
          val line = it.next();
          println("line:" + line)
        }
      }

      override def map(value: String): String = {
        value

      }
    })
    unit.print()

  }

}
