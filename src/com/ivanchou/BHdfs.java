package com.ivanchou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class BHdfs {
    public static final String LOCAL = "/Users/ivanchou/Downloads/testwrite";
    public static final String DST = "sdfs://localhost:9000/user/zxl/testwrite";
    public static final String FILEPATH = "sdfs://localhost:9000/user/zxl/test";

    public static void main(String[] args) throws IOException {
        BHdfs bHdfs = new BHdfs();
        bHdfs.read(DST);
//        bHdfs.write(LOCAL, DST);

//        RPC.stopProxy(loginService);
    }


    public void read(String fp) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.sdfs.impl", com.ivanchou.SmallFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(URI.create(fp), conf);
        Path path = new Path(fp);
        FSDataInputStream is = fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
    }

    public void write(String local, String dst) throws IOException{
        InputStream in = new BufferedInputStream(new FileInputStream(local));
        Configuration conf = new Configuration();
        conf.set("fs.sdfs.impl", com.ivanchou.SmallFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(URI.create(dst), conf);
        Path path = new Path(dst);
        if (fileSystem instanceof SmallFileSystem) {
            ((SmallFileSystem) fileSystem).create(local, path);
        } else {
            OutputStream out = fileSystem.create(path);
            IOUtils.copyBytes(in, out, 4096, true);

        }
    }
}
