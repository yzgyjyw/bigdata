package com.jyw.bigdata.hbase.crud.put;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static com.jyw.bigdata.hbase.crud.Constants.CF_INNERINFO;
import static com.jyw.bigdata.hbase.crud.Constants.F_EDUCATION;
import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

//可以获取当前插入的cell的byte[]，对于rowkey，qualifier以及value都可以通过offset加上length进行值的转换
public class CellInfo {
    static{
        //追踪源码发现其会获取hadoop.home.dir的值
        System.setProperty("hadoop.home.dir","C:\\coderapps\\hadoop-common-2.6.0-bin");
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("17f9d105-0d6f-4008-9d1b-6377bf98dee5"));
        put.add(Bytes.toBytes(CF_INNERINFO),Bytes.toBytes(F_EDUCATION),1,Bytes.toBytes("master"));
        put.add(Constants.B_CF_OUTERINFO,Constants.B_F_HEIGHT,Bytes.toBytes("170"));

        List<Cell> cells = put.get(Constants.B_CF_INNERINFO, Constants.B_F_EDUCATION);
        for (Cell cell:cells){
            //   B    $17f9d105-0d6f-4008-9d1b-6377bf98dee5	innerinfoeducation       master
            String row  = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
            String qualifier  = Bytes.toString(cell.getRowArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value  = Bytes.toString(cell.getRowArray(),cell.getValueOffset(),cell.getValueLength());
            //cell的toString格式为17f9d105-0d6f-4008-9d1b-6377bf98dee5/innerinfo:education/1/Put/vlen=6/seqid=0
            System.out.println(cell);
        }
    }
}
