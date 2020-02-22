package streamAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.customSource.NoParallel;

public class StreamingDemoComment {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source1 = env.addSource(new NoParallel());
        DataStreamSource<Long> source2 = env.addSource(new NoParallel());
        SingleOutputStreamOperator<String> map = source2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_"+value;
            }
        });
        ConnectedStreams<Long, String> connect = source1.connect(map);
        SingleOutputStreamOperator<Object> map1 = connect.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long aLong) throws Exception {
                return aLong;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });


//        DataStream<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);
        map1.print().setParallelism(1);
        String name = StreamingDemoComment.class.getSimpleName();
        env.execute(name);
    }
}
