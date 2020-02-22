package day.exprise

import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]): Unit = {
    val saprk = SparkSession.builder().appName("Spark Sql").master("local[*]").getOrCreate()



//    val frame = saprk.read.json("D:\\IDEA\\spark\\Rddexample\\src\\test\\name.json")
//    frame.show();
//    frame.createTempView("wang")
//    saprk.sql("select avg(age) from wang").show()
//    frame.select("name","age").where("age>20").show()
//    frame.filter($"age">20 &&($"age"<=29)).show()
//    frame.select($"name",$"age"+1).show()

//    val l = frame.where("age is not null").map(x=>x.getLong(0)).reduce((x,y)=>x+y)
//    println(l)



  }
}
