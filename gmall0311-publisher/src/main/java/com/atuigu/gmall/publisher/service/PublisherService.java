package com.atuigu.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map<String,Long> getDauHourCount(String date);
}
