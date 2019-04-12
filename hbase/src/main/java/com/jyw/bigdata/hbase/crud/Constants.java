package com.jyw.bigdata.hbase.crud;

import org.apache.hadoop.hbase.util.Bytes;

public interface Constants {
    String TABLE_NAME = "t1";
    String FORM_ID_TABLE = "formid";

    String CF_INNERINFO="innerinfo";
    String CF_OUTERINFO="outerinfo";

    String F_HEIGHT="height";
    String F_EDUCATION="education";

    byte[] B_CF_INNERINFO = Bytes.toBytes(CF_INNERINFO);
    byte[] B_CF_OUTERINFO = Bytes.toBytes(CF_OUTERINFO);

    byte[] B_F_HEIGHT = Bytes.toBytes(F_HEIGHT);
    byte[] B_F_EDUCATION = Bytes.toBytes(F_EDUCATION);

    byte[] B_CF_F =Bytes.toBytes("F");
    byte[] B_F_FORMID=Bytes.toBytes("D");

}
