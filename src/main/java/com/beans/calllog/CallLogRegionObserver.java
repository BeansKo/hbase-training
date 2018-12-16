package com.beans.calllog;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CallLogRegionObserver extends BaseRegionObserver {

    private Table callLogsTable;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        callLogsTable = e.getTable(TableName.valueOf("ecitem:CallLogs"));
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
    }

    /**
     * 根据主叫信息，存储相应被叫信息
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //测试
//        String putRowKey = Bytes.toString(put.getRow());
//        String ttName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
//        FileWriter fw = new FileWriter("/usr/local/hbase/call.log");
//        fw.append(ttName +"|"+putRowKey);
//        fw.flush();
//        fw.close();

        HTableDescriptor td = e.getEnvironment().getRegion().getTableDesc();
        String tableName  = td.getNameAsString();
        if("ecitem:CallLogs".equalsIgnoreCase(tableName)) {
            List<Cell> cells = put.get(Bytes.toBytes("f1"),Bytes.toBytes("CallTag"));
            if(null == cells || cells.isEmpty()){
                return;
            }

            int callTag = Bytes.toInt(CellUtil.cloneValue(cells.get(0)));
            //判断主叫
            if(callTag == 0){
                byte[] oldRow = put.getRow();
                Cell cell = put.get(Bytes.toBytes("f1"),Bytes.toBytes("OtherCallNo")).get(0);
                String otherCallNo = Bytes.toString(CellUtil.cloneValue(cell));
                cell = put.get(Bytes.toBytes("f1"),Bytes.toBytes("MyCallNo")).get(0);
                String myCallNo = Bytes.toString(CellUtil.cloneValue(cell));
                cell = put.get(Bytes.toBytes("f1"),Bytes.toBytes("CallTime")).get(0);
                String callTime = Bytes.toString(CellUtil.cloneValue(cell));
                cell = put.get(Bytes.toBytes("f1"),Bytes.toBytes("CallDur")).get(0);
                int callDur = Bytes.toInt(CellUtil.cloneValue(cell));

                String rowKey = CallLogUtil.genRowKey(otherCallNo,callTime,myCallNo,1,callDur);
                Put newPut = new Put(Bytes.toBytes(rowKey));
                newPut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("RowId"),oldRow);
                Table table = e.getEnvironment().getTable(TableName.valueOf("ecitem:CallLogs"));
                table.put(newPut);
                table.close();
            }
        }
    }

    /**
     * 根据主叫信息，查询出被叫的相应信息
     * @param e
     * @param s
     * @param results
     * @param limit
     * @param hasMore
     * @return
     * @throws IOException
     */
    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        String tableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        if(!tableName.equalsIgnoreCase("ecitem:CallLogs")) {
            return hasMore;
        }
        List<Result> newResults = new ArrayList<>();

        for(Result r: results) {
            String rowKey = Bytes.toString(r.getRow());
            //判断是主叫，不做特殊处理
            if(rowKey.contains(",0,")) {
                newResults.add(r);
            } else {
                byte[] rowId = r.getValue(Bytes.toBytes("f2"),Bytes.toBytes("RowId"));
                Get get = new Get(rowId);
                Result rr = callLogsTable.get(get);
                newResults.add(rr);
            }
        }
        results.clear();
        results.addAll(newResults);
        return hasMore;
    }
}
