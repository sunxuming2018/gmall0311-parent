package com.atuigu.gmall.publisher.service.impl;

import com.atuigu.gmall.publisher.mapper.DauMapper;
import com.atuigu.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourCount(String date) {
        List<Map> dauHourCount = dauMapper.getDauHourCount(date);
        Map<String,Long> hourMap = new HashMap<>();
        for (Map map : dauHourCount) {
            hourMap.put((String)map.get("LOGHOUR"), (Long) map.get("CT"));
        }
        return hourMap;
    }

}
