package exam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;

public class hbase {
    static Configuration conf;
    static{
        //编写配置项
       conf = new Configuration();
       conf.set("hbase.zookeeper.quorum","192.168.245.133");
       conf.set("hbase.zookeeper.proerty.clientPort","2181");
    }
    public static void main(String[] args) throws IOException {
//        createtable("exam:dept","deptinfo","childdeptinfo");
//        putdata("exam:dept","1_01","deptinfo","name","ceo");
//        putdata("exam:dept","1_01","childdeptinfo","2_02","develop");
//        putdata("exam:dept","1_01","childdeptinfo","2_05","web");
//
//        putdata("exam:dept","2_02","deptinfo","name","develop");
//        putdata("exam:dept","2_02","deptinfo","2_02","ceo");
//        putdata("exam:dept","2_02","childdeptinfo","3_03","develop1");
//        putdata("exam:dept","2_02","childdeptinfo","3_04","develop2");
//
//        putdata("exam:dept","3_03","deptinfo","name","develop1");
//        putdata("exam:dept","3_03","deptinfo","3_03","develop");
//
//        putdata("exam:dept","3_04","deptinfo","name","develop2");
//        putdata("exam:dept","3_04","deptinfo","3_04","develop");
//
//        putdata("exam:dept","2_05","deptinfo","name","web");
//        putdata("exam:dept","2_05","deptinfo","2_05","ceo");
//        putdata("exam:dept","2_05","childdeptinfo","3_06","web1");
//        putdata("exam:dept","2_05","childdeptinfo","3_07","web2");
//
//        putdata("exam:dept","3_06","deptinfo","name","web1");
//        putdata("exam:dept","3_06","deptinfo","3_06","web");

//        putdata("exam:dept","3_07","deptinfo","name","web2");
//        putdata("exam:dept","3_07","deptinfo","3_07","web");
//        sdata("exam:dept");
        seconddept("exam:dept");
    }
//    查询所有二级部门的部门编号、部门名称和下属部门的信息（显示格式如前）
    private static void seconddept(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter("2".getBytes());
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);
    }

    //    将数据插入到表中，并查询出所有部门的信息，显示格式示例如下
    private static void sdata(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);

    }
//数据展示方法
    private static void scandata(ResultScanner scanner) {
        for (Result result : scanner) {
            byte[] row = result.getRow();
            byte[] name = result.getValue("deptinfo".getBytes(),"name".getBytes());
            byte[] parent = result.getValue("deptinfo".getBytes(), row);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("childdeptinfo".getBytes());
            System.out.println("部门编号："+ Bytes.toString(row).split("_")[1]);
            System.out.println("部门名称："+Bytes.toString(name));
            if(Bytes.toString(parent)==null){
                System.out.println("上属部门：无");
            } else{
                System.out.println("上属部门："+Bytes.toString(parent));
            }
            String xia="";
            if(familyMap.isEmpty()) {
                System.out.println("下属部门：无");
            }else{
                Set<byte[]> bytes = familyMap.keySet();
                for (byte[] aByte : bytes) {
                    byte[] value = result.getValue("childdeptinfo".getBytes(), aByte);
                    xia+=Bytes.toString(value)+",";
                }
                System.out.println("下属部门："+xia.substring(0,xia.length()-1));
            }
            System.out.println("-------------------");
        }
    }

    //导入数据
    private static void putdata(String table1 ,String rowkey,String family,String qua,String value ) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(family.getBytes(),qua.getBytes(),value.getBytes());
        table.put(put);
        System.out.println("数据插入成功！");
    }

    //创建表
    private static void createtable(String table1,String... column) throws IOException {
        Admin admin = getadmin();
        //表名
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(table1));
        for (String s : column) {
            //列族名
            HColumnDescriptor col = new HColumnDescriptor(Bytes.toBytes(s));
            table.addFamily(col);
        }
        admin.createTable(table);
        System.out.println("表创建成功！");
    }


    //建立adamin类
    private static Admin getadmin() throws IOException {
        return getcon().getAdmin();
    }

    //建立连接
    private static Connection getcon() throws IOException {
      return ConnectionFactory.createConnection(conf);
    }



}
