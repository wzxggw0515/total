package exam

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import  org.apache.spark.sql.functions._
object Day17 {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    val stu = session.sparkContext.textFile("D:\\IDEA\\spark\\sparkafkaexample\\src\\main\\resources\\stu.txt")
//    12 张三 25 男 math 60
    val fields = List(
      StructField("cid",IntegerType,false),
      StructField("name",StringType,false),
      StructField("age",IntegerType,false),
      StructField("sex",StringType,false),
      StructField("sub",StringType,false),
      StructField("score",IntegerType,false)
    )
    val struct = StructType(fields)
    val data = stu.map(x => x.split(" ")).map(x => Row(x(0).toInt,
      x(1), x(2).toInt, x(3), x(4), x(5).toInt))
    val stuDF = session.createDataFrame(data,struct)
    stuDF.createTempView("stu")
//    （1）一共有多个男生参加考试？(10分)
//      println(stuDF.where($"sex"==="男").select("name").distinct().count())
//    （2）一共有多少个女生参加考试？（10分）
//    println(stuDF.where($"sex"==="女").select("name").distinct().count())
//    （3）12班有多少人参加考试？（10分）
//    println(stuDF.where($"cid"===12).select("name").distinct().count())
//    （4）13班有多少人参加考试？（10分）
//    println(stuDF.where($"cid"===13).select("name").distinct().count())
//    （5）语文科目的平均成绩是多少？（15分）
    stuDF.where($"sub"==="chinese").select(sum("score") as ("a"))
      .crossJoin(stuDF.where($"sub"==="chinese")
        .selectExpr("sum(score)/count(sub)")).show()
//    （6）数学科目的平均成绩是多少？ （15分）
    stuDF.where($"sub"==="math").select(sum("score") as ("a"))
      .crossJoin(stuDF.where($"sub"==="chinese")
        .selectExpr("sum(score)/count(sub)")).show()
//    （7）英语科目的平均成绩是多少？ （15分）
    stuDF.where($"sub"==="english").select(sum("score") as ("a"))
      .crossJoin(stuDF.where($"sub"==="chinese")
        .selectExpr("sum(score)/count(sub)")).show()
//    （8）单个人平均成绩是多少？（15分）
//    stuDF.groupBy("name").mean("score").show()
  }

}
