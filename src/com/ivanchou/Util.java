package com.ivanchou;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ivanchou on 6/2/15.
 */
public class Util {

    public static String randomFileName() {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String schema = "hdfs";
        String authority = "localhost:9000";
        return schema + "://" + authority + "/merge_" + df.format(new Date());
    }
}
