package batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * broadcast广播变量
 *
 * 需求：
 * flink会从数据源中获取到用户的姓名
 *
 * 最终需要把用户的姓名和年龄信息打印出来
 *
 * 分析：
 * 所以就需要在中间的map处理的时候获取用户的年龄信息
 *
 * 建议吧用户的关系数据集使用广播变量进行处理
 *
 * 注意：如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 *
 */
public class BatchBroadcast {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        MapOperator<Tuple2<String, Integer>, HashMap<String, Integer>> map = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        DataSource<String> data = env.fromElements("zs", "ls", "ww");
        MapOperator<String, String> str = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadcast");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }

        }).withBroadcastSet(map, "broadcast");

        str.print();


    }
}
