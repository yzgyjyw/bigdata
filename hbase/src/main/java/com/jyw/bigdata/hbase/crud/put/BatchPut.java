package com.jyw.bigdata.hbase.crud.put;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

public class BatchPut {

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

    /*当有一个put由于某些原因出错时，不会影响其它put的操作
    * */
    public static void main(String[] args) throws IOException {
        List<Put> list = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("Row1"));
        put1.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));
        Put put2 = new Put(Bytes.toBytes("Row2"));
        //在一个不存在的列族上添加cell
        //NoSuchColumnFamilyException：Column family notexist does not exist in region t1,,1550389822364.4d3592ce3e8c21faadbba2be5120c69e. in table 't1'
        put2.add(Bytes.toBytes("notexist"),Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));
        Put put3 = new Put(Bytes.toBytes("Row3"));
        put3.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));

        //put1和put3还是可以插入成功的
        list.add(put1);
        list.add(put2);
        list.add(put3);
        table.put(list);
        table.close();
    }

    private static void normalput() throws IOException {
        List<Put> list = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));
        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));
        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));

        list.add(put1);
        list.add(put2);
        list.add(put3);

        table.put(list);
        table.close();
    }


}
