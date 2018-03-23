package com.tosit.producer;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URLDecoder;

public class HttpRequestUtils {
    public static JSONObject httpPost(String url,JSONArray jsonArray){
        return httpPost(url, jsonArray, false);
    }
    public static JSONObject httpPost(String url, JSONArray jsonArray, boolean noNeedResponse){
        DefaultHttpClient httpClient = new DefaultHttpClient();
        JSONObject jsonResult = null;
        HttpPost method = new HttpPost(url);
        try {
            if (null != jsonArray) {
                StringEntity entity = new StringEntity(jsonArray.toString(), "utf-8");
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                method.setEntity(entity);
            }
            HttpResponse result = httpClient.execute(method);
            url = URLDecoder.decode(url, "UTF-8");
            if (result.getStatusLine().getStatusCode() == 200) {
                String str = "";
                try {
                    str = EntityUtils.toString(result.getEntity());
                    if (noNeedResponse) {
                        return null;
                    }
                    System.out.println(str);
                } catch (Exception e) {
                    System.out.println("post提及失败1，错误信息为："+e.toString());
                }
            }
        } catch (IOException e) {
            System.out.println("post提及失败2，错误信息为："+e.toString());
        }
        return jsonResult;
    }

    public static JSONObject httpGet(String url){
        JSONObject jsonResult = null;
        try {
            DefaultHttpClient client = new DefaultHttpClient();
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String strResult = EntityUtils.toString(response.getEntity());
                System.out.println(strResult);
                url = URLDecoder.decode(url, "UTF-8");
            } else {
                System.out.println("get提交失败1");
            }
        } catch (IOException e) {
            System.out.println("get提交失败1,错误信息为："+e.toString());
        }
        return jsonResult;
    }
}