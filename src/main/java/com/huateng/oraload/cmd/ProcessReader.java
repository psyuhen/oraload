/**
 * 
 */
package com.huateng.oraload.cmd;

import com.huateng.oraload.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;


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
@Slf4j
public class ProcessReader implements Runnable {
	private final CountDownLatch gate;
	
	private final InputStream inputStream;
	
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
		log.info(out);
		if(this.gate != null){
			this.gate.countDown();
		}
	}

}
