package day.lianxi

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object OO {
  def main(args: Array[String]): Unit = {
    val sparks = SparkSession.builder().appName("Spark Sql").master("local[*]").getOrCreate()
    import  sparks.implicits._
    val data = sparks.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\emp.txt")
    val schameString="empno,ename,job,mgr,hirdate,sal,comm,deptno"
    val schema = schameString.split(",").map(x=>StructField(x,StringType,nullable = true))
    val schem = StructType(schema)
    val datas = data.map(_.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    val frame = sparks.createDataFrame(datas,schem)
    frame.show()


  }

}
