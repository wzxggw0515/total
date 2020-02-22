package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        //需要处理的原始数据
        if(LogUtils.validateReportLog(body)){
            //如果通过则为需要的数据
            return event;
        }
        //没有通过则不为所需要数据
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        ArrayList<Event> Intercepted = new ArrayList<>(events.size());
        for (Event event : events) {
            Event intercept = intercept(event);
            if(intercept !=null){
                Intercepted.add(intercept);
            }
        }
        return Intercepted;
    }

    @Override
    public void close() {

    }
    public static  class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
