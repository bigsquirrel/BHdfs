package com.ivanchou.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileServer {

    public static final String DATA_TABLE_NAME = "DataTable";
    public static final String INDEX_TABLE_NAME = "IndexTable";
    public static final String DATA_CF_CONTENT = "content";
    public static final String DATA_CF_STATE = "state";
    public static final String INDEX_CF_STATE = "state";
    public static final String ADDRESS = "127.0.0.1";
    public static final int PORT = 9001;


    public static void main(String[] args) {
        SmallFileServer fileServer = new SmallFileServer();
        try {
            fileServer.createTables();
            fileServer.startServer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startServer() throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress(ADDRESS).setPort(PORT)
                .setInstance(new SmallFileOperate())
                .setProtocol(SmallFileOperateInterface.class);
        RPC.Server server = builder.build();
        server.start();
    }

    public void createTables() throws IOException{
        /**
         * create data table to store small file
         * row key: HEX(MD5(FileName))
         * column family: content
         * column key: content:<no> & <length>
         * column family: state
         * column key: state:lock
         */
        HBaseOperate.createTable(DATA_TABLE_NAME, new String[]{DATA_CF_CONTENT, DATA_CF_STATE});
        /**
         * create index table
         * row key: HEX(MD5(FileName))
         * column family:state
         * column key: state:exist value: 0/1
         *             state:filepath  value: path
         *             state:offset  value: <no>
         */
        HBaseOperate.createTable(INDEX_TABLE_NAME, new String[]{INDEX_CF_STATE});
    }
}
