package day.exam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Month {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error")
    val readerDF = session.read.format("jdbc").option("url", "jdbc:mysql:///1704k").option("user", "root")
      .option("password", "123").option("dbtable", "t_scores").load()
    readerDF.createTempView("stu")
    readerDF.show()
//    分别用DataFrame API和Spark SQL统计每个系，每个班所登记的各课程成绩总数，在统计结果中必须包含系、班，以及各课程成绩总数
    readerDF.groupBy("departmentid","classid").sum("language","math","english")
//      .show()
    session.sql("select departmentid,classid, sum(language),sum(math),sum(english) from stu group by departmentid,classid")
//      .show()
//    （4）分别用DataFrame API和Spark SQL统计每个系，每个班各课程的总成绩和平均成绩（保留两位小数），在统计结果中必须包含系、班的信息，以及各课程成绩。
    readerDF.groupBy("departmentid","classid").agg(sum("language"),sum("math"),sum(("english")),
      round(avg("language"),2),round(avg("math"),2),round(avg("english"),2))
//      .show()
    session.sql("select departmentid,classid, sum(language),sum(math),sum(english),round(avg(language),2),round(avg(math),2),round(avg(english),2) from stu group by departmentid,classid")
//      .show()
//    分别用DataFrame API和Spark SQL统计每个系数学成绩的前三名的学生，在统计结果中必须包含系、班和学生学号信息，以及数学成绩，要注意对相同成绩的处理
    session.sql("select departmentid,classid,studentid,math,a from(select *,dense_rank() over(partition by departmentid order by math desc )as a from stu)where a<=3")
//      .show()
      val spec = Window.partitionBy("departmentid").orderBy($"math".desc)
      readerDF.select("departmentid","classid","studentid","math")
      .withColumn("tp",dense_rank() over(spec)).where($"tp"<=3)
//      .show()
//    （6）分别用DataFrame API和Spark SQL统计每个系三课程总成绩排名前三的学生，在统计结果中必须包含系、班和学生学号信息，以及各课程成绩和三课程总成绩信息
    session.sql("select * from(select language+math+english sum,dense_rank() over(partition by departmentid order by language+math+english desc )as a ,* from stu)  where a<=3")
        .drop($"id").show()
    val spec1 = Window.  partitionBy("departmentid").orderBy($"sumall".desc)
    readerDF.withColumn("sumall",$"language"+$"math"+$"english")
      .withColumn("rk",dense_rank() over(spec1)).where($"rk"<=3).drop($"id")
      .show()

  }

}
