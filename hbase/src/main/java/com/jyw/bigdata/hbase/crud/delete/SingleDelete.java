package com.jyw.bigdata.hbase.crud.delete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

public class SingleDelete {
    static HTable table=null;
    static{
        System.setProperty("hadoop.home.dir","C:\\coderapps\\hadoop-common-2.6.0-bin");
        Configuration configuration = HBaseConfiguration.create();
        try {
            table = new HTable(configuration, TABLE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        scanOneByOne();
//        scanByMulti();
    }

    private static void scanOneByOne() throws IOException {
        long start = System.currentTimeMillis();
        ResultScanner scanner = null;
        try{
            Scan scan = new Scan();
            scanner = table.getScanner(scan);
            //迭代器，一次获取一行数据
            for(Result result:scanner){
                System.out.println(result);
            }
        }finally {
            table.close();
            if (scanner != null) {
                scanner.close();
            }
        }
        //9303
        System.out.println(System.currentTimeMillis()-start);
    }

    private static void scanByMulti() throws IOException {
        long start = System.currentTimeMillis();
        ResultScanner scanner = null;
        try{
            Scan scan = new Scan();
            scanner = table.getScanner(scan);
            //一次获取多行数据
            Result[] next = scanner.next(11);
            for(Result result:next){
                System.out.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
            if (scanner != null) {
                scanner.close();
            }
        }
        //9329,好像时间上也没有什么差别？TODO 为什么
        System.out.println(System.currentTimeMillis()-start);
    }

    private static void scanConstructor(){
        //扫描出来的结果都是按照CF:C排序的，默认情况下也只会返回当前最新版本数据
        Scan scan = new Scan();
//        scan.setFilter();
//        scan.setStartRow();
//        scan.setStopRow();
//        scan.addColumn();
//        scan.addFamily();
    }


}
