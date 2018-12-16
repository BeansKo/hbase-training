package com.beans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class HBaseScannTest {
    /**
     * 扫描设置开始结束行
     * @throws IOException
     */
    @Test
    public void testScann() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        //创建扫描器对象
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("row000010"));
        scan.withStopRow(Bytes.toBytes("row000099"));
        scan.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Id"));
        scan.addColumn(Bytes.toBytes("i"),Bytes.toBytes("Name"));
        //获得扫描器
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            //对应一行
            Result r = it.next();
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                //rowkey
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                //列族
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                //列
                String col = Bytes.toString(CellUtil.cloneFamily(cell));
                //TimeSpan
                long tims = cell.getTimestamp();
                System.out.println(row + "/" + f + "/" + col + "/" + tims);
            }
            System.out.println("############################");
        }
        rs.close();
        conn.close();
    }

    /**
     * 扫描设置缓存
     * @throws IOException
     */
    @Test
    public void testScannCatch() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        //设置scan缓存
        scan.setCaching(1000);
        System.out.println(scan.getCaching());
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        Long start = System.currentTimeMillis();
        while (it.hasNext()){
            //对应一行
            Result r = it.next();
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                //rowkey
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                //列族
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                //列
                String col = Bytes.toString(CellUtil.cloneFamily(cell));
                //TimeSpan
                long tims = cell.getTimestamp();
            }
        }
        rs.close();
        conn.close();
        Long end = new Date().getTime();
        System.out.println(end-start);
    }

    /**
     * 扫描超时重试
     * @throws IOException
     */
    @Test
    public void testScannTimeOut() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //Scan的过期时间
        //比如满足scan条件的rowkey数量为10000个，scan查询的cacheing=200，则查询所有的结果需要执行的rpc调用次数为50个。而该值是指50个rpc调用的单个相应时间的最大值
        conf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,"100");
        //Scan失败的重试次数
        System.out.println(conf.get(HConstants.HBASE_CLIENT_RETRIES_NUMBER));
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER,"3");
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        scan.setCaching(1000);
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        Long start = System.currentTimeMillis();
        while (it.hasNext()){
            //对应一行
            Result r = it.next();
//            Thread.sleep(1000);
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                //rowkey
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                //列族
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                //列
                String col = Bytes.toString(CellUtil.cloneFamily(cell));
                //TimeSpan
                long tims = cell.getTimestamp();
            }
        }
        rs.close();
        conn.close();
        Long end = new Date().getTime();
        System.out.println(end-start);
    }

    /**
     * 扫描批量
     */
    @Test
    public void testScannBatch() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        scan.setBatch(2);
        scan.setCaching(10);
        scan.withStartRow(Bytes.toBytes("row000000"));
        scan.withStopRow(Bytes.toBytes("row000010"));
        ResultScanner rs = table.getScanner(scan);
        printResult(rs);
        rs.close();
        conn.close();
    }

    /**
     * 行过滤器
     * @throws IOException
     */
    @Test
    public void testRowFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        //行过滤器，二进制比较
        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS,new BinaryComparator(Bytes.toBytes(("row000003"))));
        scan.setFilter(filter);
        //列族过滤器
        FamilyFilter fFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("i2")));
        scan.setFilter(fFilter);
        //列过滤器
        QualifierFilter qFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("Id")));
        scan.setFilter(qFilter);
        //值过滤器
        ValueFilter vFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("3"));
        scan.setFilter(vFilter);
        scan.setCaching(1000);
        scan.withStartRow(Bytes.toBytes("row000000"));
        scan.withStopRow(Bytes.toBytes("row000010"));
        ResultScanner rs = table.getScanner(scan);
        printResult(rs);
        rs.close();
        conn.close();
    }

    /**
     * FilterList
     * @throws IOException
     */
    @Test
    public void testFilterList() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable)conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        Filter ftl = new ValueFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^f"));
        Filter ftr = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(12)));
        FilterList ft = new FilterList(FilterList.Operator.MUST_PASS_ALL,ftl,ftr);

        Filter fbl = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("k$"));
        Filter fbr = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(12)));
        FilterList fb = new FilterList(FilterList.Operator.MUST_PASS_ALL, fbl, fbr);

        FilterList fall = new FilterList(FilterList.Operator.MUST_PASS_ONE,ft, fb);
        scan.setFilter(fall);
        scan.setCaching(1000);
        ResultScanner rs = table.getScanner(scan);
        printResult(rs);
        rs.close();
        conn.close();
    }

    /**
     * 单值过滤器，针对指定的列进行数据筛选
     * @throws IOException
     */
    @Test
    public void testSingleFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("i"),Bytes.toBytes("Age"),CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(20)));
        scan.setFilter(filter);
        scan.setCaching(1000);
        ResultScanner rs = table.getScanner(scan);
        printResult(rs);
        rs.close();
        conn.close();
    }

    /**
     * 分页过滤器，设置分页pagesize，有时候取的数据会大于pagesize，那是因为每次读取数据会同时从多个regionServers上读取pagesize的数量数据。
     * 所以，需要设置startkey和endkey来限制
     * @throws IOException
     */
    @Test
    public void testPageFilter() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        PageFilter pageFilter = new PageFilter(2);
        scan.setFilter(pageFilter);
        scan.setCaching(1000);
        scan.withStartRow(Bytes.toBytes("row000000"));
        scan.withStopRow(Bytes.toBytes("row000010"));
        ResultScanner rs = table.getScanner(scan);
        printResult(rs);
        rs.close();
        conn.close();
    }

    /**
     * KeyOnlyFilter只返回rowkey（坐标，rowkey/f/q/time）不会返回列的具体值信息
     * @throws IOException
     */
    @Test
    public void testKeyOnlyFilter() throws IOException{
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ecitem:FeedFlow"));
        Scan scan = new Scan();
        Filter filter = new KeyOnlyFilter();
        scan.setFilter(filter);
        scan.setCaching(1000);
        scan.withStartRow(Bytes.toBytes("row000000"));
        scan.withStopRow(Bytes.toBytes("row000010"));
        ResultScanner rs = table.getScanner(scan);
        while (rs.iterator().hasNext()){
            Result r = rs.iterator().next();
            List<Cell> cells = r.listCells();
            for(Cell cell : cells){
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                Long tims = cell.getTimestamp();
                System.out.println(row + "/" + f + "/" + col + "/" + tims);
            }
            System.out.println("============Result============");
        }
        rs.close();
        conn.close();
    }

    private void printResult(ResultScanner rs){
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value;
                if(f.equalsIgnoreCase("i")) {
                    if (col.equalsIgnoreCase("name")) {
                        value = Bytes.toString(CellUtil.cloneValue(cell));
                    } else {
                        value = String.valueOf(Bytes.toInt(CellUtil.cloneValue(cell)));
                    }
                } else {
                    value = Bytes.toString(CellUtil.cloneValue(cell));
                }
                Long tims = cell.getTimestamp();
                System.out.println(row + "/" + f + "/" + col + "/" + tims +  ":" + value);
            }
            System.out.println("============Result============");
        }
    }

    @Test
    public void myTest() {
        System.out.println(Bytes.toBytes(311659));
    }
}
