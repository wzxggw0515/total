package exam

import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._
object FinalExam {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql'").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error")
    val stuDF = session.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/1704k")
      .option("user", "root").option("password", "123").option("dbtable", "t_scores").load().toDF()

    stuDF.createTempView("stu")
//    frame.show()
//    (3)分别用DataFrame API和Spark SQL统计每个系，每个班所登记的各课程成绩总数，在统计结果中必须包含系、班，以及各课程成绩
     session.sql("select departmentid,classid,sum(language),sum(math),sum(english) from stu group by departmentid,classid").show()
      stuDF.groupBy("departmentid","classid").sum("language","math","english")
    //      .show()
//    （4）分别用DataFrame API和Spark SQL统计每个系，每个班各课程的总成绩和平均成绩（保留两位小数），在统计结果中必须包含系、班的信息，以及各课程成绩。
        session.sql("select departmentid,classid,sum(language),sum(math),sum(english)," +
        "round(avg(language),2) as chinses,round(avg(math),2) as math,round(avg(english),2) as english from stu group by departmentid,classid")
    //        .show()
          stuDF.groupBy("departmentid","classid").agg(sum("language"),sum("math"),sum("english")
          ,round(avg("language"),2) ,round(avg("math"),2) ,round(avg("english"),2))
//            .show()
//    （5）分别用DataFrame API和Spark SQL统计每个系数学成绩的前三名的学生，
//        在统计结果中必须包含系、班和学生学号信息，以及数学成绩，要注意对相同成绩的处理
        session.sql("select departmentid,classid,studentid,math from(select *, dense_rank() over(partition by departmentid order by math desc)  a from stu )b where b.a<=3")
//          .show()
       stuDF.selectExpr("departmentid","classid","studentid","math","dense_rank() over(partition by departmentid order by math desc) a").filter($"a"<=3)
//         .show()
//    （6）分别用DataFrame API和Spark SQL统计每个系三课程总成绩排名前三的学生，
//      在统计结果中必须包含系、班和学生学号信息，以及各课程成绩和三课程总成绩信息
    session.sql("select * from(select language+math+english sum, dense_rank() over(partition by departmentid order by language+math+english desc)  a ,* from stu )b where b.a<=3")
//          .show()
    stuDF.selectExpr("*","language+math+english","dense_rank() over(partition by departmentid order by language+math+english desc )a").filter($"a"<=3)
      .show()


  }

}
