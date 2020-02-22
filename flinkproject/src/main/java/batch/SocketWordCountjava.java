package batch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWordCountjava {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("wordcount --java");
            port=9000;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname="192.168.245.146";
        String delimiter="\n";
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
        DataStream<wordwithcount> windowcounts = text.flatMap(new FlatMapFunction<String, wordwithcount>() {
            public void flatMap(String value, Collector<wordwithcount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new wordwithcount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");

        windowcounts.print().setParallelism(1);
        env.execute("socket window count ");
    }

    public  static  class wordwithcount{
        public String word;
        public long count;
        public wordwithcount(){}
        public wordwithcount(String word,long count){
            this.word=word;
            this.count=count;
        }
        @Override
        public String toString() {
            return "wordwithcount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


}
