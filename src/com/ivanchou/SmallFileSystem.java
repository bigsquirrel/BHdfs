package com.ivanchou;


import com.ivanchou.server.SmallFileOperateInterface;
import com.ivanchou.server.SmallFileOperateInterface.FileStatus;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ipc.RPC;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;


/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileSystem extends DistributedFileSystem {
    private static final long SMALL_FILE_BYTES = 125000; // 1MB
    private static final long MERGE_FILE_BYTES = 1250000; // 10MB

    private SmallFileOperateInterface smallFileServer;

    @Override
    public String getScheme() {
        return "sdfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        smallFileServer = RPC.getProxy(SmallFileOperateInterface.class, 1L, new InetSocketAddress("127.0.0.1", 9001), new Configuration());
        URI hdfsUri = URI.create("hdfs" + "://" + uri.getAuthority() + "/" + uri.getPath());
        super.initialize(hdfsUri, conf);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {

        URI uri = f.toUri();
//        String filePath = uri.getPath();
        String filePath = f.toString();
        if (uri.getScheme() != null) {
            StringBuffer sb = new StringBuffer(f.toString());
            sb.setCharAt(0, 'h');
            f = new Path(sb.toString());
        }
        FileStatus status = smallFileServer.exist(filePath);

        if (status == FileStatus.EXIST) {
            // if small file is still on hbase, then return.
            InputStream is = new SFDataInputStream(smallFileServer.read(filePath));

            return new FSDataInputStream(is);
        } else if (status == FileStatus.REMOVE) {
            // if small file is merged and store to hdfs, then use Seekable interface.
            String str = smallFileServer.getHDFSFilePath(filePath);
            if (!str.equals("")) {
                String strs[] = str.split("#");
                String fp = strs[0];   // the path of the big file
                long offset = Long.valueOf(strs[1]);  // the offset in the big file
                int length = Integer.valueOf(strs[2]);

                FSDataInputStream in = super.open(new Path(fp), bufferSize);
                in.seek(offset);

                byte[] buffer = new byte[length];
                if (in.read(offset, buffer, 0, length) != length) {
                    System.out.println("-------read wrong-------");
                }
                return new FSDataInputStream(new SFDataInputStream(buffer));
            }
            return null;
        } else {
            // status == FileStatus.NOT_EXIST
            // never store to hbase, handle to hdfs.
            return super.open(f, bufferSize);
        }
    }

    public FSDataOutputStream create(String local, Path dst) throws IOException {
        File file = new File(local);
        if (file.exists() && file.length() < SMALL_FILE_BYTES) {
            // is a small file, store to hbase.
            // key:dst | value:content
            InputStream is = new FileInputStream(file);
            byte[] data = IOUtils.toByteArray(is);
            smallFileServer.write(dst.toString(), data);

            if (smallFileServer.getHBaseSize() >= MERGE_FILE_BYTES) {
                smallFileServer.merge(Util.randomFileName());
            }
            return null;
        } else {
            // not a small file, handle to hdfs.
            return this.create(dst);
        }
    }
}

