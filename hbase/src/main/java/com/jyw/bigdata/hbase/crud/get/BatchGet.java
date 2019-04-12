package com.jyw.bigdata.hbase.crud.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

/**
 * Get,以行为单位从hbase获取数据，支持batch操作
 */
public class BatchGet {
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
//        batchGet();
        exists();
    }

    private static void batchGet() throws IOException {
        Get get1 = new Get(Bytes.toBytes("Row1"));
        Get get3 = new Get(Bytes.toBytes("Row3"));
        List<Get> list = new ArrayList<>();
        list.add(get1);
        list.add(get3);
        //返回两个Result
        Result[] results = table.get(list);
        table.close();
    }

    private static void batchGetWithException() throws IOException {
        Get get1 = new Get(Bytes.toBytes("Row1"));
        Get get3 = new Get(Bytes.toBytes("RowNotExisit"));
        List<Get> list = new ArrayList<>();
        list.add(get1);
        list.add(get3);
        //与put不同，此处不会返回一个长度为1的result，而是返回null
        Result[] results = table.get(list);
        table.close();
    }

    private static void exists() throws IOException {
        Get get1 = new Get(Bytes.toBytes("Row1"));
        //判断当前指定的row是否存在于表中
        boolean exists = table.exists(get1);
        System.out.println(exists);
        table.close();
    }



}
