package exam

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import  org.apache.spark.sql.functions._
object Day18 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import session.implicits._
    val stu = session.sparkContext.textFile("D:\\IDEA\\spark\\sparkafkaexample\\src\\main\\resources\\stu.txt")
    session.sparkContext.setLogLevel("error")
    val fields = List(
      StructField("cid",IntegerType,false),
      StructField("name",StringType,false),
      StructField("age",IntegerType,false),
      StructField("sex",StringType,false),
      StructField("sub",StringType,false),
      StructField("score",IntegerType,false)
    )
    val stp = StructType(fields)
    val data = stu.map(x=>x.split(" ")).map(x=>Row(x(0).toInt,x(1),x(2).toInt,x(3),x(4),x(5).toInt))
    val stuDF = session.createDataFrame(data,stp)
    stuDF.createTempView("stu")
//    （1）12班平均成绩是多少？(10分)
      stuDF.where($"cid"===12).agg(sum("score")/countDistinct("name"))
//  .show()
//    （2）12班男生平均总成绩是多少？（10分）
        stuDF .filter($"cid" === 12 && $"sex" === "男").groupBy("name")
          .mean("score").select(sum("avg(score)"))
    //          .show()

//    （3）12班女生平均总成绩是多少？（10分）
        stuDF.where($"cid"===12).where($"sex"==="女").groupBy("name").
          avg("score").select(sum("avg(score)"))
//          .show()
//    （4）13班平均成绩是多少？(10分)
    stuDF.where($"cid"===13).agg(sum("score")/countDistinct("name"))
//      .show()

//    （5）13班男生平均总成绩是多少？（10分）
    stuDF .filter($"cid" === 13 && $"sex" === "男").groupBy("name")
      .mean("score").select(sum("avg(score)"))
//              .show()

//    13班女生平均总成绩是多少？
    stuDF .filter($"cid" === 13 && $"sex" === "女").groupBy("name")
      .mean("score").select(sum("avg(score)"))
//      .show()
//    全校语文成绩最高分是多少？

    stuDF.where($"sub"==="chinese").select(max("score"))
//      .show()

//    （8）12班语文成绩最低分是多少？（10分）
    stuDF.where($"sub"==="chinese" && $"cid"===12).select(min("score"))
//      .show()
    //    （9）13班数学最高成绩是多少？ （10分）
    stuDF.where($"sub"==="math" && $"cid"===13).select(max("score"))
//      .show()
//    （10）总成绩大于150分的12班的女生有几个？ （10分）
    stuDF.where( $"cid"===12 && $"sex"==="女").select(sum("score")as("a")).where($"a">150).select(count("*")).show()


  }
}
