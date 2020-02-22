package streaming.customSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingWithMyParallel {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new MyParellel());
        SingleOutputStreamOperator<Long> map = source.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据：" + value);
                return value;
            }
        });
        DataStream<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);
        String name = StreamingWithMyParallel.class.getSimpleName();
        env.execute(name);
    }
}
