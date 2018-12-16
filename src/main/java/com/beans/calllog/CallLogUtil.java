package com.beans.calllog;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

public class CallLogUtil {

    public static String genRowKey(String myCallNo,String callTime, String otherCallNo, int callTag, int callDur){
        String hash = getHash(myCallNo, callTime);
        String rowKey = hash + "," + myCallNo + "," + callTime + "," + otherCallNo + "," +callTag + "," + callDur;

        return rowKey;
    }

    /**
     * hashkey,确定分区
     * @param phone
     * @param callTime
     * @return
     */
    public static String getHash(String phone, String callTime){
        String last4 = phone.substring(phone.length() - 4);
        String ym = callTime.substring(0,6);
        int hash = (Integer.parseInt(ym)+Integer.parseInt(last4))%4;
        DecimalFormat df = new DecimalFormat("00");
        return df.format(hash);
    }

    public static void printResult(ResultScanner rs){
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                Long tims = cell.getTimestamp();
                System.out.println(row + "/" + f + "/" + col + "/" + tims +  ":" + value);
            }
            System.out.println("============Result============");
        }
    }
}
