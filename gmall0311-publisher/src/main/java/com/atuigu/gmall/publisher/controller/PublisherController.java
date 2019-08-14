package com.atuigu.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atuigu.gmall.publisher.service.PublisherService;
import jline.internal.Log;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Resource
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        Long dauTotal = publisherService.getDauTotal(date);

        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        List<Map> totalList = new ArrayList<>();
        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 1000);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date){
        if("dau".equals(id)){
            Map<String, Long> dauHourCountTodayMap = publisherService.getDauHourCount(date);
            String yesterday = getYesterday(date);
            Map<String, Long> dauHourYDayCountMap = publisherService.getDauHourCount(yesterday);
            Map dauMap = new HashMap();
            dauMap.put("today", dauHourCountTodayMap);
            dauMap.put("yesterday", dauHourYDayCountMap);

            return JSON.toJSONString(dauMap);
        }else if("new_mid".equals(id)){
            //新增设备业务
            return null;
        }
        return null;
    }

    private String getYesterday(String todayStr){
        String yesterdayStr = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = dateFormat.parse(todayStr);
            Date yesterday = DateUtils.addDays(today, -1);
            yesterdayStr = dateFormat.format(yesterday);
        } catch (ParseException e) {
            Log.error(e, e.getMessage());
        }
        return yesterdayStr;
    }

}
