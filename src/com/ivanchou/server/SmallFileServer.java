package com.ivanchou.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileServer {


    public static void main(String[] args) {
        SmallFileServer fileServer = new SmallFileServer();
        try {
            fileServer.startServer();
//            fileServer.createTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startServer() throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress(ServerConstant.ADDRESS).setPort(ServerConstant.PORT)
                .setInstance(new SmallFileOperate())
                .setProtocol(SmallFileOperateInterface.class);
        RPC.Server server = builder.build();
        server.start();
    }

    public void createTables() throws IOException {
        /**
         * create data table to store small file
         * row key: HEX(MD5(FileName))
         * column family: content
         * column key: content:<no> & <length>
         * column family: state
         * column key: state:lock
         */
        HBaseOperate.createTable(ServerConstant.DATA_TABLE_NAME, new String[]{ServerConstant.DATA_CF_CONTENT, ServerConstant.DATA_CF_STATE});
        /**
         * create index table
         * row key: HEX(MD5(FileName))
         * column family:state
         * column key: state:exist value: 0/1
         *             state:filepath  value: path
         *             state:offset  value: <no>
         */
        HBaseOperate.createTable(ServerConstant.INDEX_TABLE_NAME, new String[]{ServerConstant.INDEX_CF_STATE});
    }
}
