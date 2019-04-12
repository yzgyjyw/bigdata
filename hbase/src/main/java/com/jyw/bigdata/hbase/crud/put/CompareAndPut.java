package com.jyw.bigdata.hbase.crud.put;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

public class CompareAndPut {
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
        Put put = new Put(Bytes.toBytes("start01"));
        //Action's getRow must match the passed row，checkAndPut所传入的被比较的必须是与put是同一个rowkey，列族名和列名可以不一样，可以理解为是针对一行的CAS操作
        //只能检查和修改同一行数据，即保证同一行数据的原子性put
        put.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("xiao"));
        table.checkAndPut(Bytes.toBytes("start01"),Constants.B_CF_OUTERINFO,Constants.B_F_EDUCATION,null,put);
        table.close();
    }
}
