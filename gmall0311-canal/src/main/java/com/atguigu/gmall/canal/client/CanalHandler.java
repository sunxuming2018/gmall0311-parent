package com.atguigu.gmall.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.common.constant.GmallConstants;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType  entryType;
    String tableName;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType entryType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.entryType = entryType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(entryType)) {
            rowDateList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ("user_info".equals(tableName) &&
                (CanalEntry.EventType.INSERT == entryType || CanalEntry.EventType.UPDATE.equals(entryType))) {
            rowDateList2Kafka(GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private void rowDateList2Kafka(String kafkaTopic) {
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : columnsList) {
                System.out.println(column.getName() + ":::" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }
}
