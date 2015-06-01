package com.ivanchou.server;

import org.apache.avro.file.SeekableByteArrayInput;


/**
 * Created by ivanchou on 6/1/15.
 */
public interface SmallFileOperateInterface {
    static final long versionID = 1L;
    enum FileStatus {NOT_EXIST, REMOVE, EXIST}

    byte[] read(String path);

    void write();

    void delete();

    void merge();

    FileStatus exist(String path);

    String getHDFSFilePath(String path);
}
