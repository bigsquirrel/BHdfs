package com.ivanchou.server;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

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
    public void write() {

    }

    @Override
    public void delete() {

    }

    @Override
    public void merge() {

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
