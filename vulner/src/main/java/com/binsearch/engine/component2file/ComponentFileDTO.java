package com.binsearch.engine.component2file;

import com.binsearch.engine.entity.db.ComponentFile;
import lombok.Data;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class ComponentFileDTO {

//    持久化处理
    public static final Integer DATE_BASE_FLAG_SAVE_ = 1;

    public static final Integer DATE_BASE_FLAG_UN_SAVE_ = 0;


    public static final Integer SYSTEM_FILE_FLAG_SAVE_ = 1;

    public static final Integer SYSTEM_FILE_FLAG_UN_SAVE_ = 0;


    AtomicInteger dateBaseFlag; //数据库同步状态 0未同步,1已同步

    AtomicInteger systemFileFlag; //文件同步状态 0未同步,1已同步

    String curComponentFileTable;

    private ComponentFile componentFile;

    byte[] fileBytes;

    //    缓存处理
    public static final Integer CACHE_FLAG_UN_SAVE_ = 0;

    public static final Integer CACHE_FLAG_SAVE_ = 1;



    public static final Integer CACHE_ACTION_CREAT_ = 2;

    public static final Integer CACHE_ACTION_UPDATE_ = 3;


    AtomicInteger cacheFlag; //缓存状态



    public ComponentFileDTO(){
        this.cacheFlag = new AtomicInteger(0);
        this.dateBaseFlag = new AtomicInteger(0); //数据库同步状态 0未同步,1已同步
        this.systemFileFlag = new AtomicInteger(0); //文件同步状态 0未同步,1已同步
    }

    public ComponentFileDTO(ComponentFile componentFile,String curComponentFileTable,int dateBaseFlag,int systemFileFlag,int cacheFlag){
        this.componentFile = componentFile;
        this.curComponentFileTable = curComponentFileTable;
        this.dateBaseFlag = new AtomicInteger(dateBaseFlag); //数据库同步状态 0未同步,1已同步
        this.systemFileFlag = new AtomicInteger(systemFileFlag); //文件同步状态 0未同步,1已同步
        this.cacheFlag = new AtomicInteger(cacheFlag);
    }

    public int getDataBaseFlag(){
       return this.dateBaseFlag.get();
    }

    public void setDataBaseFlag(int dataBaseFlag){
        this.dateBaseFlag.set(dataBaseFlag);
    }

    public int getSystemFileFlag(){
        return this.systemFileFlag.get();
    }

    public void setSystemFileFlag(int systemFileFlag){
        this.systemFileFlag.set(systemFileFlag);
    }


    public int getCacheFlag(){
        return this.cacheFlag.get();
    }

    public void setCacheFlag(int cacheFlag){
        this.cacheFlag.set(cacheFlag);
    }

}
