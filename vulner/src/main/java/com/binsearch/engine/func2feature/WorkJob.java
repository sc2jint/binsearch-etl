package com.binsearch.engine.func2feature;

import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.engine.func2feature.service.DataBaseJobService;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
@Data
@NoArgsConstructor
public class WorkJob {


    //当前组件信息
    ComponentCacheInfo componentCacheInfo;

    //当前文件信息
    ComponentFile componentFile;

    DataBaseJobService dataBaseJobService;

    String curFuncFeatureTable;

    String language;

    String languageTableKeyValue;

    public WorkJob(ComponentCacheInfo component,
                   ComponentFile componentFile,
                   DataBaseJobService dataBaseJobService,
                   String language, String languageTableKeyValue){
        this.componentCacheInfo = component;
        this.componentFile = componentFile;
        this.dataBaseJobService = dataBaseJobService;
        this.language = language;
        this.languageTableKeyValue = languageTableKeyValue;
    }

    public void logPrintln(){

    }

    public void log(String str){
        System.out.println(str);
    }

    public void errorLog(String str){
        try {
            this.dataBaseJobService.saveFuncFeatureErrorLog(
                    this.componentCacheInfo.componentId,
                    str,
                    this.componentCacheInfo.getComponentFileName(),
                    this.getCurFuncFeatureTable());
        }catch (Exception e){}
    }
}
