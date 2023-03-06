package com.binsearch.engine.component2file;

import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.component2file.service.DataBaseJobService;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@NoArgsConstructor
public class Task {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(WorkJob.class);

    //缓存
    Cache<String, ComponentFileDTO> loadingCache;

    //日志字符串
    List<String> logs;

    //组件
    Object viewTasks;

    //缓存信息
    ComponentCacheInfo componentCacheInfo;

    //当前版本总数
    Integer versionNumCount = 0;

    //版本计数器
    AtomicInteger versionNum = new AtomicInteger(0);

    DataBaseJobService dataBaseJobService;

    public int getVersionNum(){
        return this.versionNum.get();
    }


    public void versionDecrement(){
        this.versionNum.decrementAndGet();
    }



    public Task(Object viewTasks, Integer versionNum, DataBaseJobService dataBaseJobService, ComponentCacheInfo componentCacheInfo){
        this.versionNumCount = versionNum;
        this.versionNum.set(this.versionNumCount);
        this.viewTasks = viewTasks;
        this.logs = new ArrayList<String>(){};
        this.dataBaseJobService = dataBaseJobService;
        this.componentCacheInfo = componentCacheInfo;
    }


    public void initLoadingCache(long cacheMaximum,Executor exception, RemovalListener<String,ComponentFileDTO> removalListener){
        if(Objects.isNull(this.loadingCache)) {
            this.loadingCache = Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(cacheMaximum)
                .removalListener(removalListener)
                .build();
        }
    }

    public void logPrintln(){
        logger.info("\r\n\r\n"+ StringUtils.join(logs,"\r\n"));
    }

    public void log(String str){
        System.out.println(str);
        logs.add(str);
    }

    public void errorLog(String taskId,String str){
        try {
            this.dataBaseJobService.saveComponentErrorLog(taskId, str,componentCacheInfo.componentFileName);
        }catch (Exception e){}
    }

}
