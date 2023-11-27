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
		
		if(os.contains("windows")){
			return new String[]{"cmd.exe", "/C", cmd};
		}else if(os.contains("linux") || os.contains("unix")
				|| os.contains("ux")){
			return new String[]{"/bin/sh", "-c", cmd};
		}
		
		return new String[]{cmd};
	}
}
