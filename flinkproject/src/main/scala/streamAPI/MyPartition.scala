package streamAPI

import org.apache.flink.api.common.functions.Partitioner

class MyPartition  extends  Partitioner[Long]{
  override def partition(key: Long, numPartitions: Int): Int = {
    if(key %2 ==0){
      0
    }else{1}
  }

}
