/**
 * 
 */
package com.huateng.oraload.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author ps
 *
 */
public class StreamUtil {
	private static final Log LOGGER = LogFactory.getLog(StreamUtil.class);


	public static String readProcessData(InputStream inputStream){
		InputStreamReader isr = null;
		BufferedReader br = null;
		String out = null;
		try{
			isr = new InputStreamReader(inputStream);
			br = new BufferedReader(isr);
			StringBuilder sb = new StringBuilder(1000);
			while((out = br.readLine()) != null){
				sb.append(out + "\r\n");
			}
			out = sb.toString();
		}catch (IOException e){
			LOGGER.error("读取输入流数据异常",e);
		}finally{
			if(isr != null){
				try {
					isr.close();
				} catch (IOException e) {
					LOGGER.error("关闭InputStreamReader异常",e);
				}
			}
			if(br != null){
				try {
					br.close();
				} catch (IOException e) {
					LOGGER.error("关闭BufferedReader异常",e);
				}
			}
		}
		
		return out;
	}
}
