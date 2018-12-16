package com.beans;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DecimalFormat;

/**
 * Unit test for simple App.
 */
public class HBaseTest {
    /**
     * Put数据
     * @throws Exception
     */
    @Test
    public void testHBasePut() throws Exception{
        //创建HBASE配置对象，使用的就是HADOOP的Configuration
        Configuration conf = HBaseConfiguration.create();
        //创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //得到表对象
        TableName tName = TableName.valueOf("ecitem:FeedFlow");
        Table table = conn.getTable(tName);

        byte[] rowKey = Bytes.toBytes("row2");
        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("id"),Bytes.toBytes(2));
        put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("name"),Bytes.toBytes("idea"));
        table.put(put);

        System.out.println("end");
    }

    /**
     * Get数据
     * @throws Exception
     */
    @Test
    public void testHBaseGet() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Name"));
        Result rs = table.get(get);
        byte[] bytes = rs.getValue(Bytes.toBytes("i"),Bytes.toBytes("Name"));
        System.out.println(Bytes.toString(bytes));
    }

    /**
     * 创建表空间
     * @throws Exception
     */
    @Test
    public void testNameSpace() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn =ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        NamespaceDescriptor nsd = NamespaceDescriptor.create("beans").build();
        admin.createNamespace(nsd);
        System.out.println("end");
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("beans:FeedFlow"));
        td.addFamily(new HColumnDescriptor("i"));
        admin.createTable(td);
        System.out.println("end");
    }

    /**
     * 删除表
     * @throws Exception
     */
    @Test
    public void deleteTable() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf("beans:FeedFlow"));
        admin.deleteTable(TableName.valueOf("beans:FeedFlow"));
        System.out.println("end");
    }

    /**
     * 修改表
     */
    @Test
    public void alterTable() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        admin.addColumn(TableName.valueOf("ecitem:FeedFlow"),new HColumnDescriptor("i2"));
        System.out.println("end");
    }

    /**
     * 批量put
     * @throws Exception
     */
    @Test
    public void batchPut() throws Exception{
        Long startTime = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("000000");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        //禁用自动清空缓存
        table.setAutoFlush(false);
        Put put = null;
        for(int i=0;i<1000000;i++){
            put = new Put(Bytes.toBytes("row"+df.format(i)));
            //禁用写前日志
            put.setWriteToWAL(false);
            put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Id"),Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Name"),Bytes.toBytes("Frank"+i));
            put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Age"),Bytes.toBytes(i%100));
            table.put(put);
            if(i%2000 == 0){
                table.flushCommits();
            }
        }
        table.flushCommits();
        System.out.println("时间："+(System.currentTimeMillis() - startTime));
        table.close();
        conn.close();
    }

    @Test
    public void numberFormat(){
        DecimalFormat df = new DecimalFormat("000000");
        System.out.println(df.format(123));
        System.out.println(df.format(3));
    }
}
