package com.binsearch.engine.file2feature;

import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.file2feature.service.DataBaseJobService;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import lombok.Data;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class Task {
    //缓存
    Cache<Long, FileFeatureDTO> loadingCache;

    //当前filefeature表
    String curFileFeatureTable;

    //版本计数器
    AtomicInteger workJobCount = new AtomicInteger(0);

    //当前组件信息
    ComponentCacheInfo componentCacheInfo;

    DataBaseJobService dataBaseJobService;

    String language;

    String languageTableKeyValue;

    public Task(DataBaseJobService dataBaseJobService){
        this.dataBaseJobService = dataBaseJobService;
    }

    public void initLoadingCache(long cacheMaximum,Executor exception, RemovalListener<Long,FileFeatureDTO> removalListener){
        if(Objects.isNull(this.loadingCache)) {
            this.loadingCache = Caffeine.newBuilder()
                .initialCapacity(100).maximumSize(cacheMaximum)
                .removalListener(removalListener).build();
        }
    }

    public void logPrintln(){

    }

    public void log(String str){
        System.out.println(str);
    }


    /**
     * 获取当前任务总数
     */
    public int getWorkJobCount(){
        return workJobCount.get();
    }

    /**
     * 当前任务总数 自减
     */
    public boolean workJobCountDecrement(){
        if(workJobCount.get()>0){
            workJobCount.getAndDecrement();
            return true;
        }
        return false;
    }

    /**
     * 获取当前任务总数
     */
    public void setWorkJobCount(Integer count){
        this.workJobCount.addAndGet(count);
    }


    public void errorLog(String str){
        try {
            this.dataBaseJobService.saveFileFeatureErrorLog(
                    componentCacheInfo.componentId,
                    str,
                    componentCacheInfo.getComponentFileName(),
                    curFileFeatureTable);
        }catch (Exception e){}
    }
}
