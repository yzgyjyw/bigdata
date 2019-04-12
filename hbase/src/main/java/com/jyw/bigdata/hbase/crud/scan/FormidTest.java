package com.jyw.bigdata.hbase.crud.scan;

import com.jyw.bigdata.hbase.crud.RegidUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static com.jyw.bigdata.hbase.crud.Constants.*;

public class FormidTest {

    static List<String> waitForSearch = new ArrayList<>();
    private static final int REGID_MOCK_NUM=500000;
    private static final int MAX_FORMID_PERREGID=10;

    static HTable table=null;
    static{
        System.setProperty("hadoop.home.dir","C:\\coderapps\\hadoop-common-2.6.0-bin");
        Configuration configuration = HBaseConfiguration.create();
        try {
            table = new HTable(configuration, FORM_ID_TABLE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        for(int i=0;i<50;i++){
//            putMany(REGID_MOCK_NUM/50);
//            System.out.println(i);
//        }

        List<String> strings = RegidUtils.readRegid();
        strings.stream().forEach(rowkey -> {
            try {
                getOneFormid(rowkey);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    private static void putMany(int n) throws IOException {
        List<String> regids = new ArrayList<>();
        List<Put> list = new ArrayList<>();
        for(int i=0;i<n;i++){
            String regid = UUID.randomUUID().toString();
            String time = String.valueOf(System.currentTimeMillis());
            int formidNum = MAX_FORMID_PERREGID;
            for(int j=0;j<formidNum;j++){
                Put put = new Put(Bytes.toBytes(regid+"_"+time+"_"+j));
                put.add(B_CF_F,B_F_FORMID,Bytes.toBytes(regid));
                list.add(put);
            }
            if(i%100==0){
                regids.add(regid);
            }
        }
        RegidUtils.writeRegid(regids);
        table.put(list);
//        table.close();
    }


    private  static void putDesignated() throws IOException {
        String regid = "ada8c091-1621-442b-b564-77f507ea1aa9";
//        String time = String.valueOf(System.currentTimeMillis());
        Put put = new Put(Bytes.toBytes(regid));
        String time = String.valueOf(System.currentTimeMillis());
        put.add(B_CF_F,B_F_FORMID,Bytes.toBytes(regid+"_"+time));
        table.put(put);
        table.close();
    }

    public static void getOneFormid(String rowkey) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println(start);
        String regid = rowkey;
        Scan scan = new Scan();
        String timeStart = String.valueOf(System.currentTimeMillis()-3000*1000);
        String timeEnd = String.valueOf(System.currentTimeMillis()+1);
        scan.setStartRow(Bytes.toBytes(regid+"_"+timeStart));
        scan.setStopRow(Bytes.toBytes(regid+"_"+timeEnd));
        ResultScanner scanner = table.getScanner(scan);
        if(scanner.iterator().hasNext()){
            Result next = scanner.next();
            String formid = Bytes.toString(next.getValue(B_CF_F, B_F_FORMID));
            System.out.println(formid);
            Delete delete = new Delete(next.getRow());
            table.delete(delete);
        }
        long stop = System.currentTimeMillis();
        System.out.println(stop-start);
    }
}
