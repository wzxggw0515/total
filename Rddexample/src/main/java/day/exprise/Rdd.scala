package day.exprise

import org.apache.spark.{SparkConf, SparkContext}

object Rdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark RDD example Normal")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rddept = sc.textFile("D:\\IDEA\\spark\\Rddexample\\src\\dept.txt")
    val rddemp = sc.textFile("D:\\IDEA\\spark\\Rddexample\\src\\emp.txt")
//     (20,SCOTT)
    val empkv = rddemp.map(x=>(x.split(",")(7),x.split(",")(1)))
//    (20,RESEARCH)
    val deptkv = rddept.map(x=>(x.split(",")(0),x.split(",")(1)))
//    (20,(SMITH,RESEARCH))
    val res = empkv.join(deptkv)
    val result = res.collect()
    for (elem <- result) {
      println(s"员工：${elem._2._1}->部门：${elem._2._2}")
    }

  }

}
