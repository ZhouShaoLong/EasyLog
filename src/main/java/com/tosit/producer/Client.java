package com.tosit.producer;

import org.json.JSONArray;
import org.json.JSONObject;

public class Client {
    public static void main(String[] args){
        String info = "";
        String urlPre = "http://hdp-node-0";
        String urlPost = ":55533";
        String url = "";
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        for(int i=0;i<15;i++)
        {
            info = "this info from "+((i%3)+1);
            jsonObject.put("body",info);
            jsonArray.put(jsonObject);
            url = urlPre + ((i%3)+1) + urlPost;
            System.out.println((i%3)+1);
            HttpRequestUtils.httpPost(url,jsonArray,false);
            jsonArray.remove(0);
        }
    }
}
