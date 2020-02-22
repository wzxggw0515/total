package bacthAPI

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchOuterJoin {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))


    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)
    text1.leftOuterJoin(text2).where(0).equalTo(0)
      .apply((a,b)=>{
        if(b==null){
          (a._1,a._2,"null")
        }else{
          (a._1,a._2,b._2)
        }
      }).print()
    println("----------")
    text1.rightOuterJoin(text2).where(0).equalTo(0)
      .apply((a,b)=>{
        if(a==null){
          (b._1,"null",b._2)
        }else{
          (a._1,a._2,b._2)
        }
      }).print()
    println("----------")
    text1.fullOuterJoin(text2).where(0).equalTo(0)
      .apply((a,b)=>{
        if(a==null){
          (b._1,"null",b._2)
        }else if(b==null){
          (a._1,a._2,"null")
        }else{
          (a._1,a._2,b._2)
        }
      }).print()

  }

}
