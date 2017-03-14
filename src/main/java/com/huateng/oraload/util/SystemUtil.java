/**
 * 
 */
package com.huateng.oraload.util;


/**
 * @author ps
 *
 */
public class SystemUtil {
	
	public static String[] osCmd(String cmd){
		String os = System.getProperty("os.name");
		os = os.toLowerCase();
		
		if(os.indexOf("windows") != -1){
			return new String[]{"cmd.exe", "/C", cmd};
		}else if(os.indexOf("linux") != -1 || os.indexOf("unix") != -1
				|| os.indexOf("ux") != -1){
			return new String[]{"/bin/sh", "-c", cmd};
		}
		
		return new String[]{cmd};
	}
}
