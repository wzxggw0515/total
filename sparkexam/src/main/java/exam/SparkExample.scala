package exam

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object SparkExample {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import session.implicits._
    session.sparkContext.setLogLevel("error")
    val studata = session.sparkContext.textFile("D:\\IDEA\\spark\\sparkexam\\src\\main\\resources\\access.log")
    val list = List(
      StructField("ip", StringType, false),
      StructField("redate", StringType, false),
      StructField("request", StringType, false),
      StructField("url", StringType, false),
      StructField("status", IntegerType, true),
      StructField("size", IntegerType, false)
    )
    val data = studata.map(x => x.split(" ")).map(x => Row(
      x(0), dateforamt(x(3).substring(1, x(3).length)), x(5).substring(1, x(5).length), x(6),
      x(x.length - 2).toInt, if (x(x.length - 1) == "-") 0 else x(x.length - 1).toInt
    ))
    val stp = StructType(list);
    val comDF = session.createDataFrame(data, stp);
    comDF.createTempView("inter")
    comDF.show();
//    （4）统计一分钟内访问站次数最多的前5个客户端，显示客户端名称和访问次数
    comDF.groupBy($"ip",$"redate".substr(0,12)).count().orderBy($"count".desc).limit(5).show()
//    统计从30/May/2013:17:00:00开始，至30/May/2013:23:59:59止，每小时每个客户端的访问次数
    comDF.where($"redate">="20130530170000" && $"redate"<="20130530235959").groupBy($"ip",$"redate".substr(0,10)).count()
      .show()
    session.sql("select ip,substr(redate,0,10),count(*) from inter where redate between 20130530170000 and 20130530235959 group by ip,substr(redate,0,10)")
      .show()
//    （7）统计图片资源的类型
    comDF.where($"url".contains("/image/")).map(x=>x.getString(3).substring(x.getString(3).indexOf("."),x.getString(3).length))
      .where(!$"value".contains(".swf")).distinct().show()
//    （8）统计访问图片资源的总数，及数据总大小（以MB为单位）
    comDF.where($"url".contains("/image/") && !$"url".contains(".swf")).select(count($"url"),sum($"size")/1024/1024)
      .show()
//    统计访问被缓存的图片资源的总数
      comDF.where($"url".contains("/image/")&&$"status"===304).select(count("*")).show()
//（11）网站使用swfupload.swf插件上传数据，统计文件上传的总次数
    comDF.where($"url".contains("swfupload.swf")).select(count("*")).show()
//    （9）统计各类资源被访问的总数

  }

  def dateforamt(string: String): String ={
    val format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH)
    val date = format.parse(string)
    val form = new SimpleDateFormat("yyyyMMddHHmmss",Locale.ENGLISH)
    form.format(date)
  }
}
