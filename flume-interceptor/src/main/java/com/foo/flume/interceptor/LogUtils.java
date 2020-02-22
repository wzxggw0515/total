package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    private  static Logger logger = LoggerFactory.getLogger(LogUtils.class);
    public  static boolean validateReportLog(String log){
        try {
            //1577431834641|{"cm":{"ln":"-101.4","sv":"V2.7.4","os":"8.0.2","g":"46UQ7910@gmail.com","mid":"m332","nw":"3G","l":"pt","vc":"17","hw":"640*960","ar":"MX","uid":"u824","t":"1577412369365","la":"-7.5","md":"sumsung-12","vn":"1.1.7","ba":"Sumsung","sr":"S"},"ap":"gmall","et":[]}
        //首先校验的是总长度2
            if(log.split("\\|").length<2){
                return false;
            }
            //校验|之前的是否为时间戳
            if(log.split("\\|")[0].length()!=13 || !NumberUtils.isDigits(log.split("\\|")[0])){
                return false;
            }
            //校验|之后的是否为正确的json串
            if(!log.split("\\|")[1].trim().startsWith("{")||!log.split("\\|")[1].trim().endsWith("}")){
            return false;
            }
        }catch (Exception e){

            logger.error("error parse ,message is:"+log);
            logger.error(e.getMessage());
            return false;
        }
        return  true;
    }
}
