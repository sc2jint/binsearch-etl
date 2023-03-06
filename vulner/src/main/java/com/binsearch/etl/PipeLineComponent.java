package com.binsearch.etl;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通道线程封装类
 * */
public class PipeLineComponent<T> {

    //任务去重数据获取线程数
    private int THREAD_DEF_VALUE = 0;

    //线程计数器
    private AtomicInteger threads;

    //缓存队列
    private ConcurrentLinkedQueue<T> pipeLineJobs;

    private String pipeLineName;

    public PipeLineComponent(String pipeLineName,int defValue){
        this.pipeLineName = pipeLineName;
        this.THREAD_DEF_VALUE = defValue;
        this.threads = new AtomicInteger(THREAD_DEF_VALUE);
        this.pipeLineJobs = new ConcurrentLinkedQueue<T>(){};
    }

    public String getPipeLineName(){
        return this.pipeLineName;
    }

    /**
     * 获取任务缓存大小
     */
    public int getPipeLineJobNum(){
        return pipeLineJobs.size();
    }

    public boolean isPipeLineJobNumClose(){
        return getPipeLineJobNum() == 0;
    }

    /**
     * 添加解析对象到缓存
     */
    public void addPipeLineJobs(List<T> dtos){
        pipeLineJobs.addAll(dtos);
    }



    /**
     * 添加解析对象到缓存
     */
    public void addPipeLineJob(T dto){
        pipeLineJobs.add(dto);
    }


    /**
     * 获取缓存
     */
    public T getPipeLineJobs(){
        return pipeLineJobs.poll();
    }

    /**
     * 获取取数线程数
     */
    public int getThreadNum(){
        return threads.get();
    }

    /**
     * 取数线程数自减
     */
    public boolean threadNumDecrement(){
        if(threads.get()>0){
            threads.getAndDecrement();
            return true;
        }
        return false;
    }

    /**
     *取数线程数自增
     */
    public boolean threadNumIncrement(){
        if(threads.get() < THREAD_DEF_VALUE){
            threads.getAndIncrement();
            return true;
        }
        return false;
    }

    /**
     * 清空缓存
     */
    public void clearCache(){
        pipeLineJobs.clear();
    }

    /**
     * 判断所有线程是否关闭
     * */
    public boolean isThreadClose(){
        return getThreadNum() == THREAD_DEF_VALUE;
    }
}
