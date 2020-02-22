package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取flume接收的消息
        Map<String, String> headers = event.getHeaders();
        //获取flume接受的数据组
        byte[] json = event.getBody();
        //将json转为字符串
        String str = new String(json);
        String logType="";
        if(str.contains("start")){
            logType="start";
        }else {
            logType="event";
        }
        headers.put("logType",logType);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();
        for (Event event : events) {
            Event intercept = intercept(event);
            interceptors.add(intercept);
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static  class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
