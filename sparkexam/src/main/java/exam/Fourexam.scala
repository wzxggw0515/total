package exam
import java.text.SimpleDateFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import  org.apache.spark.sql.functions._
object Fourexam {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    session.sparkContext.setLogLevel("error")
      val fields = List(
        StructField("ip",StringType,false),
        StructField("date",StringType,false),
        StructField("shuzi",StringType,false),
        StructField("req",StringType,false),
        StructField("url",StringType,false),
        StructField("http",StringType,false),
        StructField("status",IntegerType,false),
        StructField("size",IntegerType,true)
      )
    val stp = StructType(fields);
    val access = session.sparkContext.textFile("D:\\IDEA\\spark\\sparkexam\\src\\main\\resources\\access.log")
     .map(x=>x.split(" "));
        val datas = access.map(x=>Row(x(0),
          x(3).substring(1,x(3).length),x(4).substring(1,x(4).length-1),x(5).substring(1,x(5).length),x(6),x(7).substring(0,x(7).length-1),
          x(x.length-2).toInt,if(x(x.length-1)=="-") 0 else x(x.length-1).toInt
        ))
      val frameDF = session.createDataFrame(datas,stp)
        frameDF.createTempView("web")
        frameDF.show()
//    （1） 统计重定向请求数
      session.sql("select count(*) from web where status like '30%' ")
//        .show()
//    （2）统计每种响应类型的请求数，显示响应类型和请求总数
      frameDF.groupBy("req").count()
//        .show();
    session.sql("select req,count(*) from web where group by req")
//      .show()
//    （3）统计每个客户端的请求总数
    session.sql("select ip,count(url) from web group by ip")
//      .show()
//    frameDF.groupBy("ip").count().show()
//    （4）统计一分钟内访问站次数最多的前5个客户端，显示客户端名称和访问次数
        session.sql("select ip,count(ip) a from web group by ip,subStr('date',0,17) order by a desc limit 5")
//          .show()
//    （5）统计所有使用POST方法向网站提交数据的客户端，显示客户端名称和发送的URL
//    session.sql("select ip,url from web where req='POST' ").show()
//    （6）统计从30/May/2013:17:00:00开始，至30/May/2013:23:59:59止，每小时每个客户端的访问次数
         frameDF.where($"date".substr(0,20)>="30/May/2013:17:00:00" && $"date".substr(0,20)<="30/May/2013:23:59:59")
        .groupBy($"ip",$"date".substr(0,20)).count()
//        .show()
//    （7）统计图片资源的类型

//    （8）统计访问图片资源的总数，及数据总大小（以MB为单位）
      frameDF.where($"url".contains("/image/")).select(count($"url"),sum($"size")/1024/1024)
//        .show()

//    （9）统计各类资源被访问的总数
//    frameDF.groupBy($"url".substr("url".lastIndexOf("/")+1,"url".length+1)).count().show()
//    （10）统计访问被缓存的图片资源的总数
    frameDF.where($"url".contains("/image/")).select(count("*")).show()
//    （11）该网站使用swfupload.swf插件上传数据，统计文件上传的总次数
//      frameDF.where($"url".contains("swfupload.swf")).select(count("*")).show()
  }

}
