/**
 * 
 */
package com.huateng.oraload.cmd;

import com.huateng.oraload.util.StreamUtil;
import com.huateng.oraload.util.SystemUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

/**
 * @author ps
 *
 */
public class Command {
	private static final Log LOGGER = LogFactory.getLog(Command.class);
	
	public static String[]execute(String cmd){
		return execute(cmd, true);
	}
	
	/**
	 * 执行系统命令
	 * @param cmd
	 * @param isParam
	 * @return
	 */
	public static String[]execute(String cmd, boolean isParam){
		String out = "";
		String err = "";
		int exit = 1;
		try{
			Process process = Runtime.getRuntime().exec(isParam ? SystemUtil.osCmd(cmd) : new String[]{cmd});
			InputStream inputStream = process.getInputStream();
			InputStream errorStream = process.getErrorStream();
			
			out = StreamUtil.readProcessData(inputStream);
			err = StreamUtil.readProcessData(errorStream);
			
			exit = process.waitFor();
		}catch (Exception e){
			LOGGER.error(e);
		}
		
		return new String []{out,err,String.valueOf(exit)};
	}
	/**
	 * 执行系统命令，在子线程中读取
	 * @param cmd
	 * @param isParam
	 * @return
	 */
	public static String[]executeInThread(String cmd, boolean isParam){
		String out = "";
		String err = "";
		int exit = 1;
		try{
			Process process = Runtime.getRuntime().exec(isParam ? SystemUtil.osCmd(cmd) : new String[]{cmd});
			InputStream inputStream = process.getInputStream();
			InputStream errorStream = process.getErrorStream();

			CountDownLatch gate = new CountDownLatch(2);
			
			ProcessReader reader = new ProcessReader(inputStream, gate);
			Thread inputThread = new Thread(reader);
			inputThread.start();
			
			ProcessReader reader1 = new ProcessReader(errorStream, gate);
			Thread errorThread = new Thread(reader1);
			errorThread.start();

			gate.await();
			
			exit = process.waitFor();
		}catch (Exception e){
			LOGGER.error(e);
		}
		
		return new String []{out,err,String.valueOf(exit)};
	}
}
