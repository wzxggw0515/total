package day.exam

import org.apache.spark.sql.SparkSession

object Day20 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()
    import  session.implicits._
    val formation = session.sparkContext.textFile("D:\\IDEA\\spark\\Rddexample\\src\\main\\resources\\day20.txt")
    val l1 = formation.filter(x=>x.split(" ")(1)=="F").map(x=>x).count()
    println(l1)
    val l2 = formation.filter(x=>x.split(" ")(1)=="M").map(x=>x).count()
    println(l2)

    val l3 = formation.filter(x=>x.split(" ")(1)=="F" ).map(x=>x.split(" ")(2)).max()
    println(l3)
    val l4 = formation.filter(x=>x.split(" ")(1)=="M" ).map(x=>x.split(" ")(2)).max()
    println(l4)
    val l5 = formation.filter(x=>x.split(" ")(1)=="F" ).map(x=>x.split(" ")(2)).min()
    println(l5)
    val l6 = formation.filter(x=>x.split(" ")(1)=="M" ).map(x=>x.split(" ")(2)).min()
    println(l6)
  }

}
