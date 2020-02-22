package streamAPI;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.customSource.NoParallel;

import java.util.ArrayList;
/**
 * split
 *
 * 根据规则把一个数据流切分为多个流
 *
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 *
 * Created by xuwei.tech on 2018/10/23.
 */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new NoParallel()).setParallelism(1);
        SplitStream<Long> split = source.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("odd");
                } else {
                    list.add("even");
                }
                return list;
            }
        });
        DataStream<Long> odd = split.select("odd");
        DataStream<Long> even = split.select("even");
        DataStream<Long> select = split.select("odd", "even");
        select.print().setParallelism(1);
        String name = StreamingDemoSplit.class.getSimpleName();
        env.execute(name);
    }
}
