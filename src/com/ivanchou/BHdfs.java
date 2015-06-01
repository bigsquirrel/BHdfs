package com.ivanchou;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class BHdfs {

    public static void main(String[] args) throws IOException {
        BHdfs bHdfs = new BHdfs();
        bHdfs.read();


//        RPC.stopProxy(loginService);
    }


    public void read() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.sdfs.impl", com.ivanchou.SmallFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(URI.create("sdfs://localhost:9000/usr/hw1/file1"), conf);
        Path path = new Path("sdfs://localhost:9000/usr/hw1/file1");
        FSDataInputStream is = fileSystem.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
    }

    public void write() {

    }

    public void copyFromLocal(String local, String dst) {

    }

    public void readFromHadoop(String dst) {

    }
}
