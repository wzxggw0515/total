package day.lianxi


import java.text.SimpleDateFormat

import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object Example {
  def main(args: Array[String]): Unit = {
    val sparks = SparkSession.builder().appName("Spark Sql").master("local[*]").getOrCreate()
    import  sparks.implicits._
    val data = sparks.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\emp.txt")
      val fields =List(
      StructField("empno",StringType,false),
      StructField("ename",StringType,false),
      StructField("job",StringType,false),
      StructField("mgr",StringType,true),
      StructField("hirdate",StringType,false),
      StructField("sal",IntegerType,false),
      StructField("comm",IntegerType,true),
      StructField("deptno",StringType,false)
)
    val stpe = StructType(fields)

    val datas = data.map(x=>x.split(",")).map(arr=>Row(arr(0),
      arr(1),arr(2),
      arr(3),arr(4),
      arr(5).toInt,
      if(arr(6)=="NULL")0 else arr(6).toInt,
      arr(7)))
    val dataDF = sparks.createDataFrame(datas,stpe)
    dataDF.show()
    dataDF.createTempView("emp")
    //求总工资
//    dataDF.select(sum("sal")).show()
//    dataDF.selectExpr("sum(sal)").show()
//    sparks.sql("select sum(sal) from emp").show()
    //查询平均工资
//    dataDF.select(round(avg("sal"),2)).show()
//    dataDF.selectExpr("round(avg(sal),2)").show()
//    sparks.sql("select round(avg(sal),2) from emp").show()

    //查询各部门平均工资
//    dataDF.groupBy("deptno").mean("sal").show()
//    dataDF.groupBy("deptno").mean("sal")
//      .select($"deptno",round($"avg(sal)",2).as("avgsal")).show()
//    sparks.sql("select deptno,round(avg(sal),2) avgsal from emp group by deptno").show()

    // 各部门员工薪水最高的员工信息（员工编号，员工姓名，。。。。。）

//    select * from emp join(select deptno,max(sal) mxal from emp group by deptno)a on emp.sal=a.mxal and emp.deptno=a.deptno
//    sparks.sql("select * from emp where sal in(select max(sal) from emp group by deptno)")
//      .show()
//
//    val frame = dataDF.groupBy("deptno").max("sal")
//    val joinEx = dataDF.col("deptno")=== frame.col("deptno") && dataDF.col("sal")===frame.col("max(sal)")
//    dataDF.join(frame,joinEx).show()

//    dataDF.select("*").join(dataDF.groupBy("deptno").max("sal"),"deptno")
//      .where($"sal"===$"max(sal)")
//      .show()
  }

}
