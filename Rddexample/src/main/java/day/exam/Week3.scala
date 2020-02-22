package day.exam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Week3 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    val deptDF = session.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true")
      .load("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\dept.txt").toDF()
      deptDF.show()

    val empDF = session.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true")
      .load("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\emp.txt").toDF()
//      empDF.show()
//    （2）使用DataFrame API，统计每个部门员工的平均薪资
          empDF.groupBy("deptno").mean("sal").show()
//    （3）使用DataFrame API，查询各部门员工薪资大于本部门平均薪资员工信息，包括员工编号、员工姓名、入职时间和薪资
//         empDF.groupBy("deptno").mean("sal").as("a").join(empDF,"deptno")
//        .select("empno","ename","hiredate","sal").where($"sal">$"a.avg(sal)").show()
//    （4）使用DataFrame API，查询不是领导的员工信息，包括员工编号、员工姓名和入职时间
//           empDF.select($"empno")
//                 .except(empDF.select($"mgr")).join(empDF,"empno").show()
//    （5）使用DataFrame API，查询其领导是‘BLAKE’的员工信息，包括员工编号、员工姓名
        empDF.select($"empno" as "mgr").where($"ename"like("%BLAKE%"))
        .join(empDF,"mgr").select("empno","ename","mgr").show()
//    （6）使用DataFrame API，查询和SMITH工作相同的员工信息
//        empDF.select("job").where($"ename"==="SMITH").join(empDF,"job").show()
//    （7）使用DataFrame API，查询在CHICAGO工作的所有员工信息，包括员工编号、员工姓名、员工所在部门
        deptDF.select("deptno","dname").where($"loc"==="CHICAGO").join(empDF,"deptno")
        .select("empno","ename","dname","deptno").show()
//    （8）使用DataFrame API，查询10号部门的员工在整个公司中所占的比例，显示如25.23%
//          empDF.where($"deptno"==="10").select(count("ename") as("a"))
//          .crossJoin(empDF.select(count("ename").as("b")))
//          .selectExpr( "concat(round(a/b*100,2),'%')as jun" )
//          .show()






  }
}
