package com.ivanchou.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SmallFileServer {

    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1").setPort(9001)
                .setInstance(new SmallFileOperate())
                .setProtocol(SmallFileOperateInterface.class);

        RPC.Server server = builder.build();
        server.start();
    }
}
