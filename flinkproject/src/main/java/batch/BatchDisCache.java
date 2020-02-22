package batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BatchDisCache {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs或者s3上的文件
        env.registerCachedFile("d:\\data\\file\\hello.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        MapOperator<String, String> map = data.map(new RichMapFunction<String, String>() {
            private ArrayList list = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> str = FileUtils.readLines(file);
                for (String s : str) {
                    this.list.add(1);
                    System.out.println("line:" + s);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });
        map.print();

    }
}
