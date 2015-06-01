package com.ivanchou.server;

/**
 * Created by ivanchou on 6/1/15.
 */
public interface SmallFileOperateInterface {
    static final long versionID = 1L;

    String read();

    void write();

    void delete();

    void merge();

    boolean exist(String path);
}
