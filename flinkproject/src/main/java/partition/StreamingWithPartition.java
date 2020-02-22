package partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.customSource.NoParallel;

public class StreamingWithPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new NoParallel());
        env.setParallelism(2);
        //对数据进行转换，把long类型转成tuple1类型
        SingleOutputStreamOperator<Tuple1<Long>> map = source.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        DataStream<Tuple1<Long>> partitiondata = map.partitionCustom(new MyPartition(), 0);
        SingleOutputStreamOperator<Long> res = partitiondata.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程：" + Thread.currentThread().getId() + ",value:" + value);
                return value.getField(0);
            }
        });
            res.print().setParallelism(1);
            env.execute("partition");

    }
}
