package com.beans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HBaseVersionTest {

    @Test
    public void getWithVersion() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:Beans"));

        Get get = new Get(Bytes.toBytes("row001"));
        get.setMaxVersions(4);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for(Cell cell : cells) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String f = Bytes.toString(CellUtil.cloneFamily(cell));
            String col = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long tims = cell.getTimestamp();
            System.out.println(row + "/" + f + "/" + col + "/" + tims + ":" + value);
        }
        System.out.println("==========================");
        table.close();
        conn.close();
    }

    /**
     * 创建表，最大最小版本，同时设置TTL
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("ecitem:T1"));
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        //设置最大最小版本
        f1.setVersions(2,4);
        //设置TTL,15秒,默认是Integer.MAX_VALUE 永久存活
        f1.setTimeToLive(300);
        td.addFamily(f1);
        td.addFamily(new HColumnDescriptor("f2"));
        admin.createTable(td);
        admin.close();
        conn.close();
        System.out.println("ok");
    }

    /**
     * 获取TTL
     * @throws Exception
     */
    @Test
    public void getTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:Beans"));
        HTableDescriptor td = table.getTableDescriptor();
        HColumnDescriptor hd = td.getFamily(Bytes.toBytes("f1"));
        System.out.println(hd.getTimeToLive());
    }

    /**
     * 计数器
     * @throws Exception
     */
    @Test
    public void inser() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:Beans"));
        //单计数器
        table.incrementColumnValue(Bytes.toBytes("row1"),Bytes.toBytes("f1"),Bytes.toBytes("SingleClick"),10);
        //多计数器
        Increment incr = new Increment(Bytes.toBytes("row001"));
        incr.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("click"),1);
        incr.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("click1"),5);
        table.increment(incr);
        table.close();
        conn.close();
        System.out.println("ok");
    }
}
