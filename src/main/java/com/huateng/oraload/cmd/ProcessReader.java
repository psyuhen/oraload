/**
 * 
 */
package com.huateng.oraload.cmd;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

import com.huateng.oraload.util.StreamUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * @author ps
 *
 */
public class ProcessReader implements Runnable {
	private static final Log LOGGER = LogFactory.getLog(ProcessReader.class);
	private CountDownLatch gate;
	
	private InputStream inputStream;
	
	public ProcessReader(InputStream inputStream, CountDownLatch gate) {
		this.inputStream = inputStream;
		this.gate = gate;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		String out = StreamUtil.readProcessData(inputStream);
		LOGGER.info(out);
		if(this.gate != null){
			this.gate.countDown();
		}
	}

}
