/**
 * 
 */
package com.huateng.oraload.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author ps
 *
 */
@Slf4j
public class StreamUtil {
	public static String readProcessData(InputStream inputStream){
		InputStreamReader isr = null;
		BufferedReader br = null;
		String out = null;
		try{
			isr = new InputStreamReader(inputStream);
			br = new BufferedReader(isr);
			StringBuilder sb = new StringBuilder(1000);
			while((out = br.readLine()) != null){
				sb.append(out).append("\r\n");
			}
			out = sb.toString();
		}catch (IOException e){
			log.error("读取输入流数据异常",e);
		}finally{
			close(isr);
			close(br);
		}
		
		return out;
	}

	/**
	 * 关闭流
	 * @param closeable 流
	 */
	public static void close(Closeable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException e) {
			log.error("关闭流异常:"+e.getMessage(), e);
		}

	}
	/**
	 * 关闭流
	 * @param closeable 流
	 */
	public static void close(AutoCloseable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (Exception e) {
			log.error("关闭异常:", e);
		}

	}
}
