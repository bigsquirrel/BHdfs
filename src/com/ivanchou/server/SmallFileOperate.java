package com.ivanchou.server;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileOperate implements SmallFileOperateInterface {

    @Override
    public String read() {
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
        return FileStatus.EXIST;
    }

    @Override
    public String getTableName(String path) {
        return "Merge_One";
    }
}
