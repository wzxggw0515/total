package lianxi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;

public class HbaseExample {
    static Configuration conf;
    static {
         conf = new Configuration();
         conf.set("hbase.zookeeper.quorum","192.168.245.133");
         conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static void main(String[] args) throws IOException {
//        createtable("lesson:dept","deptinfo","childdeptinfo");
//        putdata("lesson:dept","1_01","deptinfo","name","ceo");
//        putdata("lesson:dept","1_01","childdeptinfo","2_02","develop");
//        putdata("lesson:dept","1_01","childdeptinfo","2_05","web");
//
//        putdata("lesson:dept","2_02","deptinfo","name","develop");
//        putdata("lesson:dept","2_02","deptinfo","2_02","ceo");
//        putdata("lesson:dept","2_02","childdeptinfo","3_03","develop1");
//        putdata("lesson:dept","2_02","childdeptinfo","3_04","develop2");
//
//        putdata("lesson:dept","3_03","deptinfo","name","develop1");
//        putdata("lesson:dept","3_03","deptinfo","3_03","develop");
//
//        putdata("lesson:dept","3_04","deptinfo","name","develop2");
//        putdata("lesson:dept","3_04","deptinfo","3_04","develop");
//
//        putdata("lesson:dept","2_05","deptinfo","name","web");
//        putdata("lesson:dept","2_05","deptinfo","2_05","ceo");
//        putdata("lesson:dept","2_05","childdeptinfo","3_06","web1");
//        putdata("lesson:dept","2_05","childdeptinfo","3_07","web2");
//
//        putdata("lesson:dept","3_06","deptinfo","name","web1");
//        putdata("lesson:dept","3_06","deptinfo","3_06","web");
//
//        putdata("lesson:dept","3_07","deptinfo","name","web2");
//        putdata("lesson:dept","3_07","deptinfo","3_07","web");

//            sdata("lesson:dept");
//            second("lesson:dept");
//            sweb("lesson:dept");
//            insertdata("lesson:dept");
            updatedata("lesson:dept");
//             String[] str = new String[]{"2_08","3_09","3_10"};
//              deldata("lesson:dept",str);
    }
//删除design
    private static void deldata(String table1,String[] str) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Delete delete =null;
        for (String s : str) {
            delete = new Delete(s.getBytes());
            table.delete(delete);
        }
        System.out.println("删除成功！");
    }

    //（6）将design调整为develop部门的下属部门，查询并显示这两个部门的信息（显示格式如前）
    private static void updatedata(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));

        Delete delete = new Delete("1_01".getBytes());
        delete.addColumn("childdeptinfo".getBytes(),"2_08".getBytes());
        table.delete(delete);

//        putdata("lesson:dept","2_02","childdeptinfo","3_08","design");
//
//        putdata("lesson:dept","3_08","deptinfo","name","design");
//        putdata("lesson:dept","3_08","deptinfo","3_08","develop");
//        putdata("lesson:dept","3_08","childdeptinfo","4_09","app-ui");
//        putdata("lesson:dept","3_08","childdeptinfo","4_10","web-ui");
//
//        putdata("lesson:dept","4_09","deptinfo","name","app-ui");
//        putdata("lesson:dept","4_09","deptinfo","4_09","design");
//
//        putdata("lesson:dept","4_10","deptinfo","name","web-ui");
//        putdata("lesson:dept","4_10","deptinfo","4_10","design");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("deptinfo".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("develop".getBytes()));
        Scan scan = new Scan();
        scan.setFilter(filter1);
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("deptinfo".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("design".getBytes()));
        scan.setFilter(filter2);
        ResultScanner scanner1 = table.getScanner(scan);
        scandata(scanner1);
    }

    //（5）新增部门design，该部门直属ceo，其下有两个部门分别是app-ui和web-ui。完成数据添加，并查询出这个部门的信息（显示格式如前）
    private static void insertdata(String table1) throws IOException {
        putdata("lesson:dept","1_01","childdeptinfo","2_08","design");

        putdata("lesson:dept","2_08","deptinfo","name","design");
        putdata("lesson:dept","2_08","deptinfo","2_08","ceo");
        putdata("lesson:dept","2_08","childdeptinfo","3_09","app-ui");
        putdata("lesson:dept","2_08","childdeptinfo","3_10","web-ui");

        putdata("lesson:dept","3_09","deptinfo","name","app-ui");
        putdata("lesson:dept","3_09","deptinfo","3_09","design");

        putdata("lesson:dept","3_10","deptinfo","name","web-ui");
        putdata("lesson:dept","3_10","deptinfo","3_10","design");
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        SingleColumnValueFilter filter = new SingleColumnValueFilter("deptinfo".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("design".getBytes()));
        Scan scan = new Scan();scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);


    }

    //    （4）查询web及其所属部门的部门编号、部门名称和下部门的信息（显示格式如前）
    private static void sweb(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        SingleColumnValueFilter filter = new SingleColumnValueFilter("deptinfo".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator("web".getBytes()));
        Scan scan = new Scan();scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);

    }

//（3）查询所有二级部门的部门编号、部门名称和下属部门的信息（显示格式如前）
    private static void second(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        PrefixFilter filter = new PrefixFilter("2".getBytes());
        Scan scan = new Scan();scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);

    }

    private static void sdata(String table1) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        scandata(scanner);
    }

    private static void scandata(ResultScanner scanner) {
        for (Result result : scanner) {
            byte[] row = result.getRow();
            byte[] name = result.getValue("deptinfo".getBytes(), "name".getBytes());
            byte[] shang = result.getValue("deptinfo".getBytes(), row);
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("childdeptinfo".getBytes());
            System.out.println("部门编号："+ Bytes.toString(row).split("_")[1]);
            System.out.println("部门名称："+Bytes.toString(name));
            if(Bytes.toString(shang)==null){
                System.out.println("上级部门：无");
            }else{
                System.out.println("上级部门："+Bytes.toString(shang));
            }
            String xia="";
              if(familyMap.isEmpty()){
                  System.out.println("下属部门：无");
              }else{
                  Set<byte[]> bytes = familyMap.keySet();
                  for (byte[] aByte : bytes) {
                      byte[] value = result.getValue("childdeptinfo".getBytes(), aByte);
                        xia+=Bytes.toString(value)+",";
                  }
                  System.out.println("下属部门："+xia.substring(0,xia.length()-1));
              }
            System.out.println("---------------");
        }
    }

    private static void putdata(String table1,String rowkey,String family,String qua,String value) throws IOException {
        Connection con = getcon();
        Table table = con.getTable(TableName.valueOf(table1));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(family.getBytes(),qua.getBytes(),value.getBytes());
        table.put(put);
        System.out.println("数据加载成功!");
    }

    //创建表
    private static void createtable(String table1,String... col) throws IOException {
        Admin admin = getadmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(table1));
        for (String s : col) {
            HColumnDescriptor column = new HColumnDescriptor(Bytes.toBytes(s));
            table.addFamily(column);
        }
        admin.createTable(table);
        System.out.println("表创建成功！");
    }

    private static Admin getadmin() throws IOException {
        return getcon().getAdmin();
    }

    private static Connection getcon() throws IOException {
       return ConnectionFactory.createConnection(conf);
    }


}
