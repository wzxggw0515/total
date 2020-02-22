package exam

import org.apache.spark.sql.SparkSession

object Six {
  def main(args: Array[String]): Unit = {
    //初始化
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error");
    val frame1 = session.read.format("csv").option("sep", " ").option("header", "true")
      .load("D:\\IDEA\\spark\\monthexam\\src\\main\\resources\\A.txt").toDF()
//    frame1.show()
    frame1.createTempView("A")
    val frame2 = session.read.format("csv").option("sep", " ").option("header", "true")
      .load("D:\\IDEA\\spark\\monthexam\\src\\main\\resources\\B.txt").toDF()
//    frame2.show()
    frame2.createTempView("B")
    val frame3 = session.read.format("csv").option("sep", " ").option("header", "true")
      .load("D:\\IDEA\\spark\\monthexam\\src\\main\\resources\\C.txt").toDF()
//    frame3.show()
    frame3.createTempView("C")
//    (1)用户登录并且当天有阅读的用户数，已经阅读书籍数量 （5分）
      frame1.join(frame2.select("*"),"user_id")
//        .show()
//    (2)用户登录并且阅读，但是没有付费的用户数
      session.sql("select * from C join (select * from A join B on A.user_id=B.user_id)as p on p.user_id != C.user_id").show()
//    (3)用户登录并且付费，付费用户书籍和金额 (5分)
    frame1.join(frame3.select("*"),"user_id")
//      .show()
  }

}
