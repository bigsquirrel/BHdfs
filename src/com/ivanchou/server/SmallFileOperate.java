package com.ivanchou.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileOperate implements SmallFileOperateInterface {

    @Override
    public byte[] read(String path) {
        try {
            return HBaseOperate.getRow(ServerConstant.DATA_TABLE_NAME, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(String rowKey, byte[] data) throws IOException{
        HBaseOperate.addRow(ServerConstant.INDEX_TABLE_NAME, rowKey, ServerConstant.INDEX_CF_STATE,
                ServerConstant.INDEX_CK_EXIST, "1"); // 1 stands for exist
        HBaseOperate.addRow(ServerConstant.DATA_TABLE_NAME, rowKey, ServerConstant.DATA_CF_CONTENT,
                rowKey, new String(data, "UTF-8"));

        long length = HBaseOperate.getTotalLength();
        length += data.length;
        // update length value
        HBaseOperate.addRow(ServerConstant.DATA_TABLE_NAME, ServerConstant.DATA_RK_LENGTH,
                ServerConstant.DATA_CF_CONTENT, ServerConstant.DATA_CK_LENGTH, String.valueOf(length));
    }

    @Override
    public void delete(String rowKey, String[] states) throws IOException{
        // update the state:exist value
        HBaseOperate.addRow(ServerConstant.INDEX_TABLE_NAME, rowKey, ServerConstant.INDEX_CF_STATE,
                ServerConstant.INDEX_CK_EXIST, "0"); // 0 stands for remove
        // add row state:filepath -> hdfs://...  in index table
        HBaseOperate.addRow(ServerConstant.INDEX_TABLE_NAME, rowKey, ServerConstant.INDEX_CF_STATE,
                ServerConstant.INDEX_CK_FILEPATH, states[0]);
        // add row state:offset -> ...  in index table
        HBaseOperate.addRow(ServerConstant.INDEX_TABLE_NAME, rowKey, ServerConstant.INDEX_CF_STATE,
                ServerConstant.INDEX_CK_OFFSET, states[1]);
        // add row state:length -> ...  in index table
        HBaseOperate.addRow(ServerConstant.INDEX_TABLE_NAME, rowKey, ServerConstant.INDEX_CF_STATE,
                ServerConstant.INDEX_CK_LENGTH, states[2]);
        // delete the row in data table
        HBaseOperate.delRow(ServerConstant.DATA_TABLE_NAME, rowKey);
        // update length value
        long length = HBaseOperate.getTotalLength();
        length -= Long.valueOf(states[2]);
        HBaseOperate.addRow(ServerConstant.DATA_TABLE_NAME, ServerConstant.DATA_RK_LENGTH,
                ServerConstant.DATA_CF_CONTENT, ServerConstant.DATA_CK_LENGTH, String.valueOf(length));
    }

    @Override
    public void merge(String dst) throws IOException{
        // read all rows in data table
        ResultScanner results = HBaseOperate.getAllRows(ServerConstant.DATA_TABLE_NAME);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        long offset = 0;
        for (Result result : results) {
            String rowKey;
            long length = 0;
            for (KeyValue kv : result.raw()) {
                rowKey = new String(kv.getRow());
                if (rowKey.equals(ServerConstant.DATA_RK_LENGTH)) {
                    continue;
                }
                length = kv.getValueLength();
                bos.write(kv.getValue());
                // delete all rows in data table
                delete(rowKey, new String[] {dst, String.valueOf(offset), String.valueOf(length)});
            }
            offset += length;
        }

        // open a FSDataOutputStream
        InputStream in = new BufferedInputStream(new ByteArrayInputStream(bos.toByteArray()));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        Path path = new Path(dst);
        OutputStream out = fs.create(path);
        // write
        IOUtils.copyBytes(in, out, 4096, true);
    }

    @Override
    public long getHBaseSize() throws IOException{
        return HBaseOperate.getTotalLength();
    }

    @Override
    public FileStatus exist(String path) {
        try {
            Result result = HBaseOperate.getResultByColumn(ServerConstant.INDEX_TABLE_NAME, path,
                    ServerConstant.INDEX_CF_STATE, ServerConstant.INDEX_CK_EXIST);
            if (!result.isEmpty()) {
                if (Integer.valueOf(new String(result.raw()[0].getValue())) == 0) {
                    return FileStatus.REMOVE;
                } else {
                    return FileStatus.EXIST;
                }
            } else {
                return FileStatus.NOT_EXIST;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return FileStatus.NOT_EXIST;
    }

    @Override
    public String getHDFSFilePath(String path) {
        Result result;
        String filePath = "";
        String offSet = "";
        String length = "";
        try {
            result = HBaseOperate.getResultByColumn(ServerConstant.INDEX_TABLE_NAME, path,
                    ServerConstant.INDEX_CF_STATE, ServerConstant.INDEX_CK_FILEPATH);
            if (!result.isEmpty()) {
                filePath = new String(result.raw()[0].getValue());
            }

            result = HBaseOperate.getResultByColumn(ServerConstant.INDEX_TABLE_NAME, path,
                    ServerConstant.INDEX_CF_STATE, ServerConstant.INDEX_CK_OFFSET);

            if (!result.isEmpty()) {
                offSet = new String(result.raw()[0].getValue());
            }

            result = HBaseOperate.getResultByColumn(ServerConstant.INDEX_TABLE_NAME, path,
                    ServerConstant.INDEX_CF_STATE, ServerConstant.INDEX_CK_LENGTH);
            if (!result.isEmpty()) {
                length = new String(result.raw()[0].getValue());
            }

            return filePath + "#" + offSet + "#" + length;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }
}
