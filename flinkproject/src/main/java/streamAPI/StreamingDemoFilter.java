package streamAPI;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.customSource.NoParallel;

public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new NoParallel());
        SingleOutputStreamOperator<Long> map = source.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始数据：" + value);
                return value;
            }
        });
        SingleOutputStreamOperator<Long> filter = map.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        SingleOutputStreamOperator<Long> map1 = filter.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据：" + value);
                return value;
            }
        });

        DataStream<Long> sum = map1.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);
        String name = StreamingDemoFilter.class.getSimpleName();
        env.execute(name);
    }
}
