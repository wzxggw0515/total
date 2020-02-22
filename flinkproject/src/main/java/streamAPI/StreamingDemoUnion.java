package streamAPI;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.customSource.NoParallel;

public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source1 = env.addSource(new NoParallel());
        DataStreamSource<Long> source2 = env.addSource(new NoParallel());
        DataStream<Long> source = source1.union(source2);
        SingleOutputStreamOperator<Long> map = source.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始数据：" + value);
                return value;
            }
        });
        DataStream<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);
        String name = StreamingDemoUnion.class.getSimpleName();
        env.execute(name);
    }
}
