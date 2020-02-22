package day.lianxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case  class Emp(empno:String,ename:String,job:String,mgr:String,hirdate:String,sal:Int,comm:Int,deptno:Int)
object Example2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
//    val depRdd = session.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\dept.txt")
    val empRdd = session.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\emp.txt")
    val empDF = empRdd.map(x=>x.split(","))
      .map(arr=>Emp(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5).toInt,
        if(arr(6)=="NULL")0 else arr(6).toInt,arr(7).toInt)).toDF()
//      empDF.show()
      empDF.createTempView("emp")

    val depDF = session.read.format("csv").option("sep",",").option("inferSchema","true").option("header","true")
      .load("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\dept.txt").toDF()
    val LevDF = session.read.format("csv").option("sep",",").option("inferSchema","true").option("header","true")
      .load("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\grade.txt").toDF()
//      depDF.show()
    depDF.createTempView("dept")
//    21.查询姓名中包含‘A’员工的姓名，编号，薪水，按照薪水降序排列
//    empDF.select("ename","empno","sal").where($"ename" like ("%A%"))
//      .orderBy($"sal"desc).show()
//    22.查询年收入超过10000的员工的姓名，编号，薪水，年收入，按照年收入降序排列
//    empDF.select($"ename",$"empno",$"sal",$"sal"*12+$"comm" as ("yearsal"))
//      .where($"yearsal" > 10000).orderBy($"yearsal" desc).show()
//    23.查询年薪超过10000的员工的姓名，编号，薪水，年收入，按照年薪降序排列
//    empDF.select($"ename",$"empno",$"sal",$"sal"*12 as ("yearsal"))
//      .where($"yearsal" > 10000).orderBy($"yearsal" desc).show()
//    24.查询雇员表中，姓名为SMITH的雇员，截止到今天共工作了多少周，则可以使用如下的SQL语句
//      empDF.select($"ename",floor(datediff(current_date(),$"hirdate")/7))
//      .where($"ename" like("%SMITH%")).show()
//    25.查询各部门的最高薪水、最低薪水、平均薪水….
//      empDF.groupBy("deptno").max("sal" )
//          .join(empDF.groupBy("deptno").min("sal"),"deptno")
//          .join(empDF.groupBy("deptno").mean("sal"),"deptno").show()
//    26.查询‘SMITH’的领导姓名
//    empDF.select($"mgr" )
//      .where($"ename" === "SMITH").as("a")
//      .join(empDF.as("b"),$"b.empno"===$"a.mgr")
//      .select("b.ename")
//      .show()
//    27.查询部门名称是‘SALES’的员工信息
//      depDF.select($"deptno" as "deptno").where($"dname"==="SALES").join(empDF,"deptno")
//        .select("*").show()
//    28.查询公司中薪水最高的员工信息
//      empDF.select(max("sal")as "sal").join(empDF,"sal")
//        .select("empno","ename","job","mgr","hirdate","sal","comm","deptno")
//        .show()
//    29.查询公司所有员工的个数
//    empDF.select(count("*") as "总人数" ).show()
//    30.查询公司中最高薪水是多少
//    empDF.select(max("sal")as "最高薪水").show()
//    51.查询最高领导者的薪水等级

//    52.查询薪水最低的员工信息
//      empDF.select(min("sal").as("sal"))
//        .join(empDF,"sal")
//        .select("*").show()
//    53.查询和SMITH工作相同的员工信息
//      empDF.select($"job" as "job").where($"ename"==="SMITH")
//      .join(empDF.select("*"),"job").show()
//    54.查询不是领导的员工信息
//    55.查询平均工资比10部门低的部门编号
      empDF.groupBy("deptno").mean("sal").as("a")
          .crossJoin(empDF.where($"deptno"===10).select(avg("sal")).as("b"))
          .where($"b.avg(sal)">$"a.avg(sal)").select("a.deptno").show()
//         empDF.where($"deptno"===10).select(avg("sal")as "sal").as("b")
//    56.查询在纽约工作的所有员工
//    57.查询‘SALES’部门平均薪水的等级
//    58.查询10号部门的员工在整个公司中所占的比例：
//    60.查询各部门工资大于该部门平均工资的员工信息：
  }

}
