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
 * @Class : ProcessReader
 * @Package : com.huateng.oraload.cmd
 * @Description : TODO
 * @Author : sam.pan
 * @Create : 2017/9/22 17:01
 * @version V1.0
 * @ModificationHistory Who      When        What
 * =============     ==============  ==============================
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
