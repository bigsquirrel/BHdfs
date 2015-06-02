package com.ivanchou.server;

import java.io.IOException;

/**
 * Created by ivanchou on 6/1/15.
 */
public interface SmallFileOperateInterface {
    static final long versionID = 1L;
    enum FileStatus {NOT_EXIST, REMOVE, EXIST}

    byte[] read(String path);

    void write(String rowKey, byte[] data) throws IOException;

    void delete(String rowKey, String[] states) throws IOException;

    void merge(String dst) throws IOException;

    long getHBaseSize() throws IOException;

    FileStatus exist(String path);

    String getHDFSFilePath(String path);
}
