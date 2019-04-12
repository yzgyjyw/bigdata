package com.jyw.bigdata.hbase.crud.put;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

/**
 * 为什么需要客户端缓存？
 * 每一次的put操作其实就是一个RPC的调用，需要尽可能减少网络开销时间
 */
public class ClientCache {
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


    //客户端flush的原理：在客户端将put操作按region分开，然后分别与该region服务器进行RPC传输数据

    /*缓冲区的大小：一个较大的缓冲区意味着服务端和客户端都要需要更大的内存消耗，因为服务端也需要将传输过来的写入到
        缓冲区，然后再处理它，估算服务端的内存占用：
        hbase.regionserver.handler.count（regionserver上用于等待响应用户表级请求的线程数）
        hbase.regionserver.handler.count*hbase.client.write.buffer*region服务器数量
    */

    /**关于刷新，可以分为手动flush和被动flush:
     *      手动 flush：flushCommit()
     *      被动 等待客户端缓冲区满自动flush
     */
    public static void main(String[] args) throws IOException {
        //默认情况下客户端缓冲区是禁用的，设置autoFlash为false开启客户端缓冲区
        table.setAutoFlush(false);
        Put put = new Put(Bytes.toBytes("1"));
        put.add(Constants.B_CF_INNERINFO,Constants.B_F_EDUCATION,Bytes.toBytes("bachelor"));
        table.put(put);
        //table.flushCommits();

        //获得客户端缓冲区的大小
        /*若要设置客户端缓冲区大小，可以调用set方法
        *或者当不想为用户创建的每一个table分别设置缓冲区大小，而是要设置全局客户端的缓冲区的大小，hbase-site.xml:
        *<property>
            <name>hbase.client.write.buffer</name>
            <value>2097152</value>  2M
         </property>
        **/
        long size = table.getWriteBufferSize();

        //在调用close方法时，会调用flushCommits方法
        table.close();
    }
}
