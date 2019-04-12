package com.jyw.bigdata.hbase.crud.get;

import com.jyw.bigdata.hbase.crud.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static com.jyw.bigdata.hbase.crud.Constants.TABLE_NAME;

public class SingleGet {

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
//        singleGet2();
        getInfoFromResult();
    }

    private  static void singleGet() throws IOException {
        //一个get只能获取一行的数据，但是不限制列族和列
        Get get = new Get(Bytes.toBytes("Row1"));
        get.addFamily(Constants.B_CF_INNERINFO);
        Result result = table.get(get);
        String value = Bytes.toString(result.getValue(Constants.B_CF_INNERINFO, Constants.B_F_EDUCATION));
        System.out.println(value);
        table.close();
    }

    private static void singleGet2() throws IOException {
        //默认情况下get获取的是当前row中所有的最新的单元格的数据
        Get get = new Get(Bytes.toBytes("Row1"));
        //想要获取指定的列的数据，按照singleGet中调用相应get方法即可
        //多个version，则调用setMaxVersions
//        get.setMaxVersions(3);
        Result result = table.get(get);
        System.out.println(result.size());
        table.close();
    }

    private static void getInfoFromResult() throws IOException {
        Get get = new Get(Bytes.toBytes("Row1"));
        Result result = table.get(get);
        //value返回的是该row中按顺序排列的第一个cell的最新值，列的排序为CF:C
        String value = Bytes.toString(result.value());

        //该方法没有时间戳参数，只能获取指定cell的最新的值
        result.getValue(Constants.B_CF_INNERINFO, Constants.B_F_EDUCATION);
        //本次get获取的cell的个数,由于本次get没有指定maxVersion，所以size的值为2
        result.size();

        //这两个方法内部是一样的，只是返回的集合不一样，内部都已经将cell排好序了
        Cell[] cells = result.rawCells();
        List<Cell> list = result.listCells();

        table.close();
    }



}