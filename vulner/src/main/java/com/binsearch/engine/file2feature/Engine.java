package com.binsearch.engine.file2feature;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.Constant;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.file2feature.service.AnalysisJobService;
import com.binsearch.engine.file2feature.service.DataBaseJobService;
import com.binsearch.engine.file2feature.service.ExtractJobService;
import com.binsearch.etl.EngineComponent;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
public class Engine implements ETLService {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");
    Logger logger = LoggerFactory.getLogger(Engine.class);

    @Autowired
    @Qualifier(EngineConfiguration.FILE2FEATURE_DATABASE_JOB_SERVICE)
    DataBaseJobService dataBaseJobService;

    @Autowired
    @Qualifier(EngineConfiguration.FILE2FEATURE_FEATUREEXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(EngineConfiguration.FILE2FEATURE_RECORD_SERVICE)
    AnalysisJobService analysisJobService;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    // 118:[v7.6.0.zip,...]@...格式
    @Value("#{'${base.service.specify.components}'.split('@')}")
    List<String> components;


    @Value("#{'${file2feature.source.system.language}'.split(',')}")
    List<String> languages;

    public final Map<String,String> languagesMap = new HashMap<String,String>(){};

    public final ConcurrentLinkedQueue<String> cacheQueue = new ConcurrentLinkedQueue<String>();

    //目标表数
    @Value("${file2feature.target.system.table.count}")
    Integer tableCount;

    //抽取表数
    @Value("${file2feature.source.system.table.count}")
    Integer sourceTableCount;

    @Value("${file2feature.source.system.pageSize}")
    Integer pageSize;

    @Override
    public void init() throws Exception {
        if(CollectionUtils.isEmpty(languages)){
            throw new Exception("file2feature language is empty");
        }

        if(languagesMap.isEmpty()) {
            for (String language : languages) {
                Constant.LanguageType languageType = Constant.LanguageType.getLanguageTypeForLanguageName(language);
                if(Objects.nonNull(languageType)) {
                    languagesMap.put(languageType.getLanguageName(), languageType.getAlias());
                }else{
                    throw new Exception("file2feature language is empty:"+language);
                }
            }

            languagesMap.forEach((key, value) -> {
                dataBaseJobService.createFileFeature(value, tableCount);
            });

            if (!CollectionUtils.isEmpty(components)) {
                cacheQueue.addAll(components);
            }
        }
    }

    @Override
    public void incrementalComponent() {
        int curPage = 1;
        cacheQueue.clear();

        while(true) {
            try {
                if(cacheQueue.size()>(pageSize*0.8)){
                    try{
                        TimeUnit.MILLISECONDS.sleep(50L);
                    }catch (Exception e){}
                    continue;
                }

                List<ComponentCacheInfo> cacheInfos = dataBaseJobService.getComponentCount((curPage-1)*pageSize, pageSize);

                if(CollectionUtils.isEmpty(cacheInfos)){
                    break;
                }

                cacheInfos.forEach(item -> {
                    //获取需要进行跟新的组件
                    if(item.getComponent2fileFlag() == ComponentCacheInfo.RUNNING_FLAGS_END &&
                            item.getFile2featureFlag() == ComponentCacheInfo.RUNNING_FLAGS_UN_END ) {
                        cacheQueue.add(item.getComponentId());
                    }
                });

                specifyComponent();

            } catch (Exception e) {}
            curPage++;
        }
        unResources(engineComponent);
    }

    @Override
    public void specifyComponent() {
        while(true){
            String componentId = null;
            try {
                componentId = cacheQueue.poll();
                if(Strings.isBlank(componentId)){
                    break;
                }

                componentId = componentId.contains(":")?componentId.split(":")[0]:componentId;
                ComponentCacheInfo componentCacheInfo = dataBaseJobService.getComponentInfo(componentId);

                if(Objects.isNull(componentCacheInfo)){
                    error_log_.info("找不到组件缓存componentId ={}",componentCacheInfo.componentId);
                    continue;
                }

                if(Strings.isBlank(componentCacheInfo.getComponentFileName()) ||
                        componentCacheInfo.getComponent2fileFlag() == ComponentCacheInfo.RUNNING_FLAGS_UN_END){
                    error_log_.info("组件 componentId = {} 未完成文件提取",componentCacheInfo.componentId);
                    continue;
                }

//              将状态跟新为未结束
                dataBaseJobService.updateComponentCacheFlag(componentId,ComponentCacheInfo.RUNNING_FLAGS_UN_END);

//               对组件中的不同语言进行处理
                for(int i = 0;i < languages.size();i++){
                    int curPage = 1;//当前页数
                    Task task = new Task(dataBaseJobService);
                    task.setLanguageTableKeyValue(languagesMap.get(languages.get(i)));
                    task.setLanguage(languages.get(i));
                    task.setComponentCacheInfo(componentCacheInfo);

                    while (engineComponent.isJobRun()) {
                        try {
                            if(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).getPipeLineJobNum()>(pageSize*0.8)){
                                try{
                                    TimeUnit.MILLISECONDS.sleep(10L);
                                }catch (Exception e){}
                                continue;
                            }

                            List<Object> jobs = dataBaseJobService.getComponentJobs((curPage-1)*pageSize, pageSize,task);
                            if(CollectionUtils.isEmpty(jobs)){
                                while(engineComponent.
                                        getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                                    extractJobService.featureExtract();
                                }

                                while(engineComponent.
                                        getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                                    analysisJobService.analysisExtract();
                                }
                                break;
                            }
                            engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                            engineComponent.setCurWorkJobCount(jobs.size());
                            task.setWorkJobCount(jobs.size());

                            while(engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                                extractJobService.featureExtract();
                            }

                            while(engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                                analysisJobService.analysisExtract();
                            }

                        } catch (Exception e) {
                            error_log_.info("分页装载任务出错,component.id = {},{}",componentCacheInfo.componentId,e.getMessage());
                        }
                        curPage++;
                    }
                }

                //将状态跟新为结束
                dataBaseJobService.updateComponentCacheFlag(componentId,ComponentCacheInfo.RUNNING_FLAGS_END);

            }catch (Exception e) {
                error_log_.info("获取组件信息失败,component.id = {},{}",componentId,e.getMessage());
            }finally {
                unResources(engineComponent);
            }
        }
        unResources(engineComponent);
    }


    public void fullComponent() {
        String table = "t_component_file";
        FileFeatureEngineInfo fileFeatureEngineInfo = null;
        try {
            fileFeatureEngineInfo = dataBaseJobService.getFileFeatureEngineInfo();
        }catch (Exception e){
            error_log_.info("file2feature模块初始化失败,{}",e.getMessage());
            return;
        }

        //根据指定语言,遍历所有的t_component_file表
        int index = languages.indexOf(fileFeatureEngineInfo.language);
        for(int i = index == -1 ? 0 : index;i < languages.size();i++){
            try {
                fileFeatureEngineInfo.language = languages.get(i);
                fileFeatureEngineInfo.languageTableKeyValue = languagesMap.get(fileFeatureEngineInfo.language);

                int num = Integer.parseInt(fileFeatureEngineInfo.curFileTable.substring(table.length()));

                //遍历t_component_file表
                for(int x = num == 0 ? 1 : num;x<=sourceTableCount;x++){
                    fileFeatureEngineInfo.curFileTable = String.format("%s%s", table,x);
                    fullComponent(fileFeatureEngineInfo);
                    dataBaseJobService.saveFileFeatureEngineInfo(fileFeatureEngineInfo);
                }
                fileFeatureEngineInfo.curFileTable = String.format("%s%s", table, 1);

            }catch (Exception e){
                error_log_.info("遍历指定语言存储表失败,languages = {},table = {},{}",fileFeatureEngineInfo.language,fileFeatureEngineInfo.curFileTable,e.getMessage());
            }finally {
                unResources(engineComponent);
            }
        }
    }

    private void fullComponent(FileFeatureEngineInfo engineInfo) {
        List<ComponentCacheInfo> components = null;
        try {
            //获取当前表,指定语言的组件信息
            components = dataBaseJobService.getComponentCaches(engineInfo.curFileTable);
        } catch (Exception e) {
            error_log_.info("\n\n ********************* {} 引擎初始化失败 \n\n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        }

        if(CollectionUtils.isEmpty(components)){
            return;
        }


        components.forEach(component -> {
            try{
                int curPage = 1;//当前页数

                dataBaseJobService.updateComponentCacheFlag(component.componentId, ComponentCacheInfo.RUNNING_FLAGS_UN_END);

                Task task = new Task(dataBaseJobService);
                task.setLanguageTableKeyValue(engineInfo.languageTableKeyValue);
                task.setLanguage(engineInfo.language);
                task.setComponentCacheInfo(component);

                while (engineComponent.isJobRun()) {
                    try {
                        if(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).getPipeLineJobNum()>(pageSize*0.8)){
                            try{
                                TimeUnit.MILLISECONDS.sleep(10L);
                            }catch (Exception e){}
                            continue;
                        }

                        List<Object> jobs = dataBaseJobService.getComponentJobs((curPage-1)*pageSize, pageSize,task);
                        if(CollectionUtils.isEmpty(jobs)){
                            while(engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                                extractJobService.featureExtract();
                            }

                            while(engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                                analysisJobService.analysisExtract();
                            }
                            break;
                        }
                        engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                        engineComponent.setCurWorkJobCount(jobs.size());
                        task.setWorkJobCount(jobs.size());

                        while(engineComponent.
                                getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                            extractJobService.featureExtract();
                        }

                        while(engineComponent.
                                getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                            analysisJobService.analysisExtract();
                        }


                    } catch (Exception e) {
                        error_log_.info("分页装载任务出错,component.id = {},{}",component.componentId,e.getMessage());
                    }finally {
                        System.out.printf("分页加载任务完成,component.id = %s,curPage = %s%n",component.componentId,curPage);
                        curPage++;
                    }
                }

                dataBaseJobService.updateComponentCacheFlag(component.componentId, ComponentCacheInfo.RUNNING_FLAGS_END);
            } catch (Exception e) {}finally {
                unResources(engineComponent);
            }
        });
    }
}
