package com.beans.calllog;

import com.beans.calllog.CallLogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestPutCallLog {
    @Test
    public void put() throws IOException {
        Configuration conf  = HBaseConfiguration.create();
        conf.set("hbase.regionserver.lease.period","60000");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:CallLogs"));

        String myCallNo = "18991266098";//主叫号码
        String callTime = "201812091702";//呼叫时间
        String otherCallNo = "18991268200";//被叫号码
        int callTag = 0;//0:主叫，1:被叫
        int callDur = 120;//通话时间

        String rowKey = CallLogUtil.genRowKey(myCallNo,callTime,otherCallNo, callTag,callDur);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("MyCallNo"),Bytes.toBytes(myCallNo));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("OtherCallNo"),Bytes.toBytes(otherCallNo));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CallTime"),Bytes.toBytes(callTime));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CallDur"),Bytes.toBytes(callDur));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CallTag"),Bytes.toBytes(0));
        table.put(put);
        table.close();
        conn.close();
        System.out.println("ok");
    }

    /**
     * 查询指定号码在固定月份的数据。
     * @throws IOException
     */
    @Test
    public void  findMonthCallLogs() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:CallLogs"));
        Scan scan = new Scan();
        String phnoeNo = "18991268200";
        String startMonth = "201812";
        String endMonth = "201901";
        String hashRegion = CallLogUtil.getHash(phnoeNo,startMonth);
        String startKey = hashRegion+","+phnoeNo+","+startMonth;
        String endKey = hashRegion+","+phnoeNo+","+endMonth;
        scan.withStartRow(Bytes.toBytes(startKey));
        scan.withStopRow(Bytes.toBytes(endKey));
        ResultScanner rs = table.getScanner(scan);
        CallLogUtil.printResult(rs);
        table.close();
        conn.close();
    }
}
