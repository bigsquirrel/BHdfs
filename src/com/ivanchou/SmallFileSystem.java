package com.ivanchou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;


/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileSystem extends DistributedFileSystem {
    @Override
    public String getScheme() {
        return "sdfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        System.out.println("-------> " + uri.toString());
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        System.out.println("++++++>" + f.toString());

        return super.open(f, bufferSize);
    }


}

