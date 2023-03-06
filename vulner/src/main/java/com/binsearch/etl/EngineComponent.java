package com.binsearch.etl;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineComponent<T> {

    //开关锁
    public static final Object SWITCH_LOCK = new Object();

    //任务运行锁，防止多次启动
    private static final AtomicBoolean lockRunJob = new AtomicBoolean(false);


    private  HashMap<String, PipeLineComponent<T>> jobPipeLines;

    public EngineComponent(HashMap<String, PipeLineComponent<T>> jobPipeLines){
        this.jobPipeLines = jobPipeLines;
    }

    //当前任务总数
    private static final AtomicInteger curWorkJobCount = new AtomicInteger(0);


    /**
     * 获取当前任务总数
     */
    public int getCurWorkJobCount(){
        return curWorkJobCount.get();
    }

    /**
     * 当前任务总数 自减
     */
    public boolean curWorkJobCountDecrement(){
        if(curWorkJobCount.get()>0){
            curWorkJobCount.getAndDecrement();
            return true;
        }
        return false;
    }

    /**
     * 获取当前任务总数
     */
    public void setCurWorkJobCount(Integer count){
        this.curWorkJobCount.addAndGet(count);
    }


    /**
     * 任务是否被锁定
     */
    public boolean isJobRun(){
        return lockRunJob.get();
    }

    /**
     * 任务解锁
     */
    public void unRunJob(){
        lockRunJob.set(false);
    }

    /**
     * 任务加锁
     */
    public void runJob(){
        lockRunJob.set(true);
    }


    /**
     * 判断队列线程是否释放
     * */
    public PipeLineComponent<T> getPipeLineComponent(String key){
        return jobPipeLines.get(key);
    }

    /**
     * 判断队列线程是否释放
     * */
    public boolean isThreadClose(){
        for(PipeLineComponent<T> component:jobPipeLines.values()){
            if (!component.isThreadClose()){
                return false;
            }
        }
        return true;
    }


    public boolean isPipeLineJobNumClose(){
        for(PipeLineComponent<T> component:jobPipeLines.values()){
            if (!component.isPipeLineJobNumClose()){
                return false;
            }
        }
        return true;
    }

    /**
     * 清空缓存
     */
    public void clearCache(){
        for(PipeLineComponent<T> component:jobPipeLines.values()){
            component.clearCache();
        }
    }
}
