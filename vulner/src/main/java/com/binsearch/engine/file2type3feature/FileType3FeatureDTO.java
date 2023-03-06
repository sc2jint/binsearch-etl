package com.binsearch.engine.file2type3feature;

import com.binsearch.engine.entity.db.FileType3Feature;

import java.util.concurrent.atomic.AtomicInteger;

public class FileType3FeatureDTO {

    //当前数据存储表
    String curFileType3FeatureTable;

    //当前数据
    private FileType3Feature fileType3Feature;


    //持久化处理
    AtomicInteger dateBaseFlag; //数据库同步状态 0未同步,1已同步

    public static final Integer DATE_BASE_FLAG_SAVE_ = 1;

    public static final Integer DATE_BASE_FLAG_UN_SAVE_ = 0;



    //缓存处理
    AtomicInteger cacheFlag; //缓存状态

    public static final Integer CACHE_FLAG_UN_SAVE_ = 0;

    public static final Integer CACHE_FLAG_SAVE_ = 1;



    public FileType3FeatureDTO(){
        this.cacheFlag = new AtomicInteger(0);
        this.dateBaseFlag = new AtomicInteger(0); //数据库同步状态 0未同步,1已同步
    }

    public FileType3FeatureDTO(FileType3Feature fileType3Feature, String curFileFeatureTable, int dateBaseFlag, int cacheFlag){
        this.fileType3Feature = fileType3Feature;
        this.curFileType3FeatureTable = curFileFeatureTable;
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


    public String getCurFileType3FeatureTable() {
        return curFileType3FeatureTable;
    }

    public void setCurFileType3FeatureTable(String curFileType3FeatureTable) {
        this.curFileType3FeatureTable = curFileType3FeatureTable;
    }

    public FileType3Feature getFileType3Feature() {
        return fileType3Feature;
    }

    public void setFileType3Feature(FileType3Feature fileType3Feature) {
        this.fileType3Feature = fileType3Feature;
    }

}
