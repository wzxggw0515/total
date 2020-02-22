package day.exprise

import org.apache.spark.sql.SparkSession
case class Data(str:String)
object Day11 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import session.implicits._
    val data = session.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\day11.txt")

    data.distinct().sortBy(x=>x.split(",")(0).toInt).toDF().show()

    data.distinct().sortBy(x=>x.split(",")(0).toInt).map(x=>Data(x)).toDS().show()


  }
}
