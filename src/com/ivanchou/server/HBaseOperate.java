package com.ivanchou.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ivanchou on 6/1/15.
 */
public class HBaseOperate {

    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    public static void createTable(String tableName, String[] columnFamilys) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            System.out.println("table is already exist!");
            System.exit(0);
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String columnFamily : columnFamilys) {
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            }
            hAdmin.createTable(tableDesc);
            System.out.println("create table success!");
        }
    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);

        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("delete table success!");
        } else {
            System.out.println("table is not exist!");
            System.exit(0);
        }
    }

    public static void addRow(String tableName, String row,
                              String columnFamily, String column, String value) throws IOException {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
    }

    public static void delRow(String tableName, String row) throws IOException {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    public static void delMultiRows(String tableName, String[] rows)
            throws Exception {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();

        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            list.add(del);
        }

        table.delete(list);
    }

    public static byte[] getRow(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        for (KeyValue kv : result.raw()) {
            System.out.print("Row Name: " + new String(kv.getRow()) + " ");
            System.out.print("Timestamp: " + kv.getTimestamp() + " ");
            System.out.print("column Family: " + new String(kv.getFamily()) + " ");
            System.out.print("Row Name:  " + new String(kv.getQualifier()) + " ");
            System.out.println("Value: " + new String(kv.getValue()) + " ");
        }
        return result.raw()[0].getValue();
    }

    public static long getTotalLength() throws IOException{
        byte[] data = getRow(ServerConstant.DATA_TABLE_NAME, ServerConstant.DATA_RK_LENGTH);
        return Long.valueOf(new String(data));
    }

    public static Result getResultByColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        return table.get(get);
    }

    public static ResultScanner getAllRows(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

//        for (Result result : results) {
//            for (KeyValue rowKV : result.raw()) {
//                System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
//                System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
//                System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
//                System.out
//                        .print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
//                System.out.println("Value: " + new String(rowKV.getValue()) + " ");
//            }
//        }
        return results;
    }
}
