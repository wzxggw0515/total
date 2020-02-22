package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String jsonkeysString) {

        // 0 准备一个 sb
        StringBuilder sb = new StringBuilder();
        // 1 切割 jsonkeys mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");
        // 2 处理 line 服务器时间 | json
        String[] logContents = line.split("\\|");
        // 3 合法性校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }
        // 4 开始处理 json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            // 获取 cm 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");
            // 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String filedName = jsonkeys[i].trim();
                if (base.has(filedName)) {
                    sb.append(base.getString(filedName)).append("\t");
                } else {
                    sb.append("").append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line ="1577866224538|{\"cm\":{\"ln\":\"-115.8\",\"sv\":\"V2.3.2\",\"os\":\"8.2.2\",\"g\":\"S5861R7W@gmail.com\",\"mid\":\"m276\",\"nw\":\"WIFI\",\"l\":\"es\",\"vc\":\"18\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"u702\",\"t\":\"1577846127420\",\"la\":\"-22.3\",\"md\":\"HTC-19\",\"vn\":\"1.1.4\",\"ba\":\"HTC\",\"sr\":\"O\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1577854992966\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"1\",\"newsid\":\"n075\",\"news_staytime\":\"10\",\"loading_time\":\"12\",\"action\":\"2\",\"showtype\":\"4\",\"category\":\"76\",\"type1\":\"\"}},{\"ett\":\"1577776689051\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"0\",\"action\":\"2\",\"extend1\":\"\",\"type\":\"2\",\"type1\":\"433\",\"loading_way\":\"1\"}},{\"ett\":\"1577775388082\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"0\",\"action\":\"1\",\"detail\":\"102\",\"source\":\"1\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"2\"}},{\"ett\":\"1577862000881\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"3\"}},{\"ett\":\"1577811912370\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"1\"}},{\"ett\":\"1577767661491\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1577830075552\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1577861624764\",\"praise_count\":281,\"other_id\":3,\"comment_id\":4,\"reply_count\":186,\"userid\":3,\"content\":\"颊蚤净的谣秀虐寥覆\"}},{\"ett\":\"1577813102089\",\"en\":\"praise\",\"kv\":{\"target_id\":4,\"id\":6,\"type\":1,\"add_time\":\"1577831582576\",\"userid\":5}}]}\n";

        String x = new BaseFieldUDF().evaluate(line,
                "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }

    }
