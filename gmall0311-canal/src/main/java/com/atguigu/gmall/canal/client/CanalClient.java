package com.atguigu.gmall.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        CanalClient client = new CanalClient();
        client.watch("hadoop113", 11111, "example", "gmall0311.*");
    }

    public void watch(String hostname,int port, String destination, String tables){
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("没有数据!!!休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        String tableName = entry.getHeader().getTableName();
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }
    }

}
