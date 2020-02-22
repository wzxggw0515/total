package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inpath="D:\\data\\file";
        String outpath="D:\\data\\count";
        DataSource<String> source = env.readTextFile(inpath);
        AggregateOperator<Tuple2<String, Integer>> sum = source.flatMap(new Tokenizer()).groupBy(0).sum(1);
        sum.writeAsCsv(outpath,"\n"," ").setParallelism(1);
        env.execute("count");
    }

    public static class  Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] strings = value.toLowerCase().split("\\s+");
            for (String token : strings) {
            if(token.length()>0){
                out.collect(new Tuple2<String, Integer>(token,1));
            }
            }
        }
    }
}
