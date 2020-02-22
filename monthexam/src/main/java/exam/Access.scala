package exam

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object Access {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error");
    //读取数据
    val data = session.sparkContext.textFile("D:\\IDEA\\spark\\monthexam\\src\\main\\resources\\access.log")
    //建立表字段
    val list = List(
      StructField("ip",StringType,false),
      StructField("redate",StringType,false),
      StructField("requ",StringType,false),
      StructField("url",StringType,false),
      StructField("status",IntegerType,true),
      StructField("size",IntegerType,true)
    )
    val structType = StructType(list)
    //切割数据
    val da = data.map(x => x.split(" ")).map(x => Row(x(0),
      x(3).substring(1, x(3).length), x(5).substring(1, x(5).length), x(6),
      x(x.length - 2).toInt, if(x(x.length - 1)=="-")0 else x(x.length - 1).toInt))
   //字段映射
    val frameDF = session.createDataFrame(da,structType)
    frameDF.createTempView("inter")
//    frameDF.show()

//    统计一分钟内访问站次数最多的前5个客户端，显示客户端名称和访问次数（5分）
      frameDF.groupBy($"ip",$"redate".substr(0,17)).count().orderBy($"count".desc).limit(5)
//      .show()
//    ）统计从30/May/2013:17:00:00开始，至30/May/2013:23:59:59止，每小时每个客户端的访问次数（5分）
frameDF.where($"redate".substr(0,14)>="30/May/2013:17" && $"redate".substr(0,14)<="30/May/2013:23").
        groupBy($"ip",$"redate".substr(0,14)).count()
//        .show()
//    统计访问被缓存的图片资源的总数
    frameDF.where($"url".contains("/image/") && $"status"===304).select(count("url"))
//      .show()

//    (2)请使用sparkRDD的方式分析各个种类category的总销售额（5分）
      val dts = session.sparkContext.textFile("D:\\IDEA\\spark\\monthexam\\src\\main\\resources\\five.txt")
      dts.map(x=>x.split(" ")(2)).map(x=>x.split(":")).map(x=>(x(0),x(2).toInt))
      .reduceByKey((a,b)=>a+b).foreach(println)



  }

}
