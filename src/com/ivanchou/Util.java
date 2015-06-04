package com.ivanchou;

import org.apache.hadoop.fs.Path;

import java.net.URI;
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

    public static Path toHDFS(Path p) {
        URI uri = p.toUri();
        if (uri.getScheme() != null) {
            StringBuffer sb = new StringBuffer(p.toString());
            sb.setCharAt(0, 'h');
            p = new Path(sb.toString());
        }
        return p;
    }
}
