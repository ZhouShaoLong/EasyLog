package com.tosit.producer;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Client {
    public static void main(String[] args){
        //模拟数据生成
        int[] userIds = {2000,2000,2001};
        long begintime = 1488326400000l;
        long endtime = 1488327000000l;
        String[] packages = {"com.browser","com.browser1","com.browser2"};
        String urlPre = "http://hdp-node-0";
        String urlPost = ":55533";
        String url = "";
        Random random = new Random();
        for (int i=0;i<6;i++){
            JSONObject obj = new JSONObject();
            JSONArray arr = new JSONArray();
            JSONObject data1 = new JSONObject();
            JSONObject data2 = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            JSONObject jsonObject = new JSONObject();

            //数据生成
            obj.put("userId",userIds[random.nextInt(3)]);
            obj.put("day","2017-02-01");
            obj.put("begintime",begintime+600000l*i);
            obj.put("endtime",endtime+600000l*i);
            data1.put("package",packages[random.nextInt(3)]);
            data1.put("activetime",1);
            data2.put("package",packages[random.nextInt(3)]);
            data2.put("activetime",100);
            arr.put(data1);
            arr.put(data2);
            obj.put("data",arr);
            String info = obj.toString();

            jsonObject.put("body",info);
            jsonArray.put(jsonObject);
            url = urlPre + ((i%3)+1) + urlPost;
            System.out.println((i%3)+1);
            HttpRequestUtils.httpPost(url,jsonArray,false);
        }
    }
}
