package com.ivanchou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class BHdfs {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.sdfs.impl", com.ivanchou.SmallFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(URI.create("sdfs://localhost:9000/usr/zxl/file1"), conf);
        Path path = new Path("sdfs://localhost:9000/usr/zxl/file1");
        fileSystem.open(path);

//        RPC.stopProxy(loginService);
    }

    public void copyFromLocal(String local, String dst) {

    }

    public void readFromHadoop(String dst) {

    }
}
