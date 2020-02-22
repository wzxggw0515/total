package day.exam

import org.apache.spark.sql.SparkSession
case class Stu(name:String,sub:String,score:Int)
object Day12 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
   session.read.format("jdbc").option("url","jdbc:mysql://192.168.245.133:3306/hivedb")
      .option("dbtable","student").option("user","root").option("password","123456").load()
      .toDF().show()

  }

}
