package com.huateng.oraload.pool;

import lombok.Getter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @version V1.0
 * @Project : oraload
 * @Package : com.huateng.oraload.pool
 * @Description : 线程池
 * @Author : sam.pan
 * @Create : 2018/1/16 17:39
 * @ModificationHistory Who      When        What
 * =============     ==============  ==============================
 */
public class ThreadPool {
    private int maxSize = 5;
    private @Getter ExecutorService executorService;

    private ThreadPool(){
        initPool(maxSize);
    }

    public static class ThreadPoolHolder {
        public static ThreadPool instance = new ThreadPool();
    }

    public static ThreadPool getPool(){
        return ThreadPoolHolder.instance;
    }

    /**
     * 初始化化线程池
     * @param maxSize 最大线程数
     */
    public void initPool(int maxSize){
        maxSize = (maxSize == 0) ? this.maxSize: maxSize;
        executorService = Executors.newFixedThreadPool(maxSize);
    }

    private void reInitPool(){
        if(executorService == null){
            initPool(this.maxSize);
        }
    }
    /**
     * 使用executor来执行
     * @param runnable 任务
     */
    public void execute(Runnable runnable){
        reInitPool();
        executorService.execute(runnable);
    }

    /**
     * 关闭
     */
    public void shutdown(){
        if(executorService != null){
            executorService.shutdown();
            executorService = null;
        }
    }
}
