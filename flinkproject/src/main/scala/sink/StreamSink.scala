package sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object StreamSink {
  def main(args: Array[String]): Unit = {
    val port =9000
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("flink2",port,'\n')
    import org.apache.flink.api.scala._

    val l_scala = text.map(line=>("l_scala",line))
    val conf = new FlinkJedisPoolConfig.Builder().setHost("flink1").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String,String]](conf,new mySink)
    l_scala.addSink(redisSink)

    //执行任务
    env.execute("Socket window count");


  }

  class  mySink extends  RedisMapper[Tuple2[String,String]]{
    override def getKeyFromData(t: (String, String)): String = {
      t._1
    }

    override def getValueFromData(t: (String, String)): String = {
      t._2
    }

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

  }}
