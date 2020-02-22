package exam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Month {
  def main(args: Array[String]): Unit = {
    //初始化
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error");
    //连接jdbc
    val stuDF = session.read.format("jdbc").option("url", "jdbc:mysql:///1704k").option("user", "root")
      .option("password", "123").option("dbtable", "t_scores").load().toDF()
    stuDF.show()
    stuDF.createTempView("stu");
//    分别用DataFrame API和Spark SQL统计每个系数学成绩的前三名的学生，
//    在统计结果中必须包含系、班和学生学号信息，以及数学成绩，要注意对相同成绩的处理（5分）
    session.sql("select departmentid,classid,studentid,math,a " +
      "from (select *,dense_rank() over(partition by departmentid order by math desc) a from stu) where a<=3")
      .show()
      val spec = Window.partitionBy("departmentid").orderBy($"math".desc)
     stuDF.select("departmentid","classid","studentid","math")
      .withColumn("tp",dense_rank() over(spec)).where($"tp"<=3)
      .show()

  }

}
