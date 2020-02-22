package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> list = new ArrayList<>();
        list.add(12);
        list.add(20);
        list.add(28);
        DataStreamSource<Integer> coll = env.fromCollection(list);
        SingleOutputStreamOperator<Object> num = coll.map(new MapFunction<Integer, Object>() {
            @Override
            public Object map(Integer value) throws Exception {
                return value + 1;
            }
        });
        num.print().setParallelism(1);
        env.execute("clooection");

    }

}
