package day.exprise

import org.apache.spark.sql.SparkSession

case class Spc(id:Int,sub:String,addr:String)
object Test{
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import session.implicits._
    val sc = session.sparkContext
    val sprd = sc.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\dept.txt").map(_.split(","))
      .map(x => Spc(x(0).toInt, x(1), x(2))).toDF()
    sprd.createTempView("person")
    session.sql("select * from person where id>20").show()


  }
}
