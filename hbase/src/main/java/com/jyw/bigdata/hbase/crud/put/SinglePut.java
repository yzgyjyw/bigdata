package com.jyw.bigdata.hbase.crud.put;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import static com.jyw.bigdata.hbase.crud.Constants.*;

/**
 * 疑惑：创建的列族的versions=3，但是针对同一行的同一列增加了不止3个版本，直接scan 't1', {RAW => true, VERSIONS => 10}还是会出现不止3个
 * 原因一：没有compact
 * 原因二：compact之前没有flush到磁盘
 *
 * 关于版本的注意事项：
 * 通常最好让regionserver负责跟踪版本号，而不是自己在add的时候指定timestamp（自己可以指定timestamp为1,2,3等）
 * 如果需要自己指定timestamp，一定要注意timestamp的数值，hbase在存储的时候是根据该值进行倒序存储，timestamp小的在最后
 * 如果当前cell的version数已经大于max version，那么后面的（timesatmp值较小）cell会先被删除
 */
public class SinglePut {
    static{
        //追踪源码发现其会获取hadoop.home.dir的值
        System.setProperty("hadoop.home.dir","C:\\coderapps\\hadoop-common-2.6.0-bin");
    }
    public static void main(String[] args) throws IOException {
        getPutInfo();
    }

    private static void put01() throws IOException {
        //System.out.println(System.getenv("HADOOP_HOME"));
        //该Configuration类与hadoopConfiguration一致，在创建的时候，其会去加载classpath下的hbase-default.xml和hbase-site.xml
        Configuration configuration = HBaseConfiguration.create();
        //若不需要配置文件，也可以直接在代码中设置相关的属性
        //configuration.set("name","value");

        HTable table = new HTable(configuration, TABLE_NAME);
        //创建一个put，以rowkey作为条件
        Put put = new Put(Bytes.toBytes("17f9d105-0d6f-4008-9d1b-6377bf98dee5"));
        //向put中指定的rowkey增加一个单元格，一个单元格（rowkey:CF:F:version）
        put.add(Bytes.toBytes(CF_INNERINFO),Bytes.toBytes(F_EDUCATION),Bytes.toBytes("master"));
        //将增加的cell插入到table中
        table.put(put);
        table.close();
    }

    //重复对一行添加元素，测试version
    private static void putbyonerow() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("17f9d105-0d6f-4008-9d1b-6377bf98dee5"));
        put.add(Bytes.toBytes(CF_INNERINFO),Bytes.toBytes(F_EDUCATION),1,Bytes.toBytes("master"));
        table.put(put);
        table.close();
    }

    //获取put中添加的cell的信息
    private static void keyvalue() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("1"));
        put.add(Constants.B_CF_INNERINFO,Bytes.toBytes(F_HEIGHT),Bytes.toBytes("173"));
        put.add(Constants.B_CF_OUTERINFO,Bytes.toBytes(F_EDUCATION),Bytes.toBytes("bachelor"));
        //获取put中添加的单元格信息，treemap，key：rowkey，keyvalue就是cell的信息,注意是针对列族进行划分的
        NavigableMap<byte[], List<KeyValue>> putFamilyMap = put.getFamilyMap();
        //获取put中添加的单元格的信息,可能有多个版本
        List<Cell> putCell = put.get(Bytes.toBytes(CF_INNERINFO), Bytes.toBytes(F_HEIGHT));
        System.out.println("debugger");
    }

    private static void getPutInfo() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("1"));
        put.add(Bytes.toBytes(CF_INNERINFO),Bytes.toBytes(F_HEIGHT),Bytes.toBytes("173"));
        put.add(Bytes.toBytes(CF_OUTERINFO),Bytes.toBytes(F_EDUCATION),Bytes.toBytes("bachelor"));
        put.numFamilies();  //put.getFamilyMap返回的map的size，即本次put操作针对的列族的数目
        put.heapSize();     //当前put的所需堆的大小
        put.size();         //当前put中cell的数目，注意针对同一个cell的多次操作算多次
        put.getTimeStamp(); //若未指定为Long.MAX_VALUE
        put.getId();
        System.out.println("debugger");
    }



}
