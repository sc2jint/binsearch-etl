package com.binsearch.engine.file2feature;

import com.binsearch.engine.entity.db.FileFeature;
import java.util.concurrent.atomic.AtomicInteger;

public class FileFeatureDTO {

    //当前数据存储表
    String curFileFeatureTable;

    //当前数据
    private FileFeature fileFeature;


    //持久化处理
    AtomicInteger dateBaseFlag; //数据库同步状态 0未同步,1已同步

    public static final Integer DATE_BASE_FLAG_SAVE_ = 1;

    public static final Integer DATE_BASE_FLAG_UN_SAVE_ = 0;



    //缓存处理
    AtomicInteger cacheFlag; //缓存状态

    public static final Integer CACHE_FLAG_UN_SAVE_ = 0;

    public static final Integer CACHE_FLAG_SAVE_ = 1;


    public static final Integer CACHE_ACTION_CREAT_ = 2;

    public static final Integer CACHE_ACTION_UPDATE_ = 3;

    public FileFeatureDTO(){
        this.cacheFlag = new AtomicInteger(0);
        this.dateBaseFlag = new AtomicInteger(0); //数据库同步状态 0未同步,1已同步
    }

    public FileFeatureDTO(FileFeature fileFeature, String curFileFeatureTable, int dateBaseFlag, int cacheFlag){
        this.fileFeature = fileFeature;
        this.curFileFeatureTable = curFileFeatureTable;
        this.dateBaseFlag = new AtomicInteger(dateBaseFlag); //数据库同步状态 0未同步,1已同步
        this.cacheFlag = new AtomicInteger(cacheFlag);
    }

    public int getDataBaseFlag(){
        return this.dateBaseFlag.get();
    }

    public void setDataBaseFlag(int dataBaseFlag){
        this.dateBaseFlag.set(dataBaseFlag);
    }


    public int getCacheFlag(){
        return this.cacheFlag.get();
    }

    public void setCacheFlag(int cacheFlag){
        this.cacheFlag.set(cacheFlag);
    }


    public String getCurFileFeatureTable() {
        return curFileFeatureTable;
    }

    public void setCurFileFeatureTable(String curFileFeatureTable) {
        this.curFileFeatureTable = curFileFeatureTable;
    }

    public FileFeature getFileFeature() {
        return fileFeature;
    }

    public void setFileFeature(FileFeature fileFeature) {
        this.fileFeature = fileFeature;
    }

}
