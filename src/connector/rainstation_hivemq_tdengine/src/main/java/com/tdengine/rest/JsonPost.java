package com.tdengine.rest;

import com.common.ReadProperties;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Base64;


public class JsonPost {
	static String base64 = null;
	public static void HttpPostWithJson(String sql) {

		CloseableHttpClient httpClient = null;
		String returnValue = "{\"code\":500,\"msg\":\"接口调用失败\"}"; //"这是默认返回值，接口调用失败";

		//ResponseHandler<String> responseHandler = new BasicResponseHandler();
		try {
			//第一步：  创建HttpClient对象
			httpClient = HttpClients.createDefault();

			//第二步：创建httpPost对象
			HttpPost httpPost = new HttpPost(ReadProperties.read("REST_URL"));

			//第三步：给httpPost设置JSON格式的参数
			StringEntity requestEntity = new StringEntity(sql, "utf-8");
			requestEntity.setContentEncoding("UTF-8");

			 if( base64 == null){
				 String username = ReadProperties.read("PROPERTY_KEY_USER");
				 String password = ReadProperties.read("PROPERTY_KEY_PASSWORD");
				 String text = username+":"+password;
				 final Base64.Encoder encoder = Base64.getEncoder();
				 final byte[] textByte = text.getBytes("UTF-8");
				 base64 = encoder.encodeToString(textByte);
			 }
			httpPost.setHeader("Authorization", "Basic "+base64);
			httpPost.setEntity(requestEntity);

			//第四步：发送HttpPost请求，获取返回值
			httpClient.execute(httpPost); //调接口获取返回值时，必须用此方法

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				httpClient.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}