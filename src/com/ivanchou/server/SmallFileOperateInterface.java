package com.ivanchou.server;

/**
 * Created by ivanchou on 6/1/15.
 */
public interface SmallFileOperateInterface {
    static final long versionID = 1L;
    enum FileStatus {NOT_EXIST, REMOVE, EXIST}

    String read();

    void write();

    void delete();

    void merge();

    FileStatus exist(String path);

    String getTableName(String path);
}
