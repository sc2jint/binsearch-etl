package com.binsearch.engine.func2feature;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.Constant;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.func2feature.service.DataBaseJobService;
import com.binsearch.engine.func2feature.service.ExtractJobService;
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
    @Qualifier(EngineConfiguration.FUNC2FEATURE_DATABASE_JOB_SERVICE)
    DataBaseJobService dataBaseJobService;

    @Autowired
    @Qualifier(EngineConfiguration.FUNC2FEATURE_FEATUREEXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Value("#{'${func2feature.source.system.language}'.split(',')}")
    List<String> languages;

    public final Map<String,String> languagesMap = new HashMap<String,String>(){};


    @Value("#{'${base.service.specify.components}'.split('@')}")
    List<String> components;


    public final ConcurrentLinkedQueue<String> cacheQueue = new ConcurrentLinkedQueue<String>();


    @Value("${func2feature.target.system.table.count}")
    Integer tableCount;

    @Value("${func2feature.source.system.table.count}")
    Integer sourceTableCount;

    @Value("${func2feature.source.system.pageSize}")
    Integer pageSize;

    @Override
    public void init() throws Exception {
        if(CollectionUtils.isEmpty(languages)){
            throw new Exception("func2feature languages is empty");
        }

        if(languagesMap.isEmpty()) {
            for (String language : languages) {
                Constant.LanguageType languageType  = Constant.LanguageType.getLanguageTypeForLanguageName(language);
                if(Objects.nonNull(languageType)) {
                    languagesMap.put(languageType.getLanguageName(), languageType.getAlias());
                }else{
                    throw new Exception("func2feature language is empty:"+language);
                }
            }

            languagesMap.forEach((key, value) -> {
                dataBaseJobService.createFuncFeature(value, tableCount);
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
                        TimeUnit.MILLISECONDS.sleep(10L);
                    }catch (Exception e){}
                    continue;
                }

                List<ComponentCacheInfo> cacheInfos = dataBaseJobService.getComponentCount((curPage-1)*pageSize, pageSize);

                if(CollectionUtils.isEmpty(cacheInfos)){
                    break;
                }

                cacheInfos.forEach(item -> {
                    if(item.getComponent2fileFlag() == ComponentCacheInfo.RUNNING_FLAGS_END &&
                            item.getFunc2featureFlag() == ComponentCacheInfo.RUNNING_FLAGS_UN_END ) {
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

                dataBaseJobService.updateComponentCacheFlag(componentId,ComponentCacheInfo.RUNNING_FLAGS_UN_END);

                for(int i = 0;i < languages.size();i++) {
                    int curPage = 1;//当前页数
                    while (engineComponent.isJobRun()) {
                        try {
                            if (engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).getPipeLineJobNum() > (pageSize * 0.8)) {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(30L);
                                } catch (Exception e) {}
                                continue;
                            }


                            List<Object> jobs = dataBaseJobService.getComponentJobs((curPage - 1) * pageSize, pageSize,
                                    languages.get(i), languagesMap.get(languages.get(i)), componentCacheInfo);

                            if (CollectionUtils.isEmpty(jobs)) {
                                while (engineComponent.
                                        getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()) {
                                    extractJobService.featureExtract();
                                }
                                break;
                            }

                            engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                            engineComponent.setCurWorkJobCount(jobs.size());


                            while (engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()) {
                                extractJobService.featureExtract();
                            }

                            curPage++;
                        } catch (Exception e) {
                            error_log_.info("分页装载任务出错,component.id = {},{}", componentCacheInfo.componentId, e.getMessage());
                        }
                    }
                }

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
        FuncFeatureEngineInfo funcFeatureEngineInfo = null;
        try {
            funcFeatureEngineInfo = dataBaseJobService.getFuncFeatureEngineInfo();

        }catch (Exception e){
            error_log_.info("file2feature模块初始化失败,{}",e.getMessage());
            return;
        }

        //根据指定语言,遍历所有的t_component_file表
        int index = languages.indexOf(funcFeatureEngineInfo.language);
        for(int i = index == -1 ? 0 : index;i < languages.size();i++){
            try {
                funcFeatureEngineInfo.language = languages.get(i);
                funcFeatureEngineInfo.languageTableKeyValue = languagesMap.get(funcFeatureEngineInfo.language);

                int num = Integer.parseInt(funcFeatureEngineInfo.curFileTable.substring(table.length()));

                //遍历t_component_file表
                for(int x = num == 0 ? 1 : num;x<=sourceTableCount;x++){
                    funcFeatureEngineInfo.curFileTable = String.format("%s%s", table,x);
                    fullComponent(funcFeatureEngineInfo);
                    dataBaseJobService.saveFuncFeatureEngineInfo(funcFeatureEngineInfo);
                }
                funcFeatureEngineInfo.curFileTable = String.format("%s%s", table, 1);

            } catch (Exception e){
                error_log_.info("遍历指定语言存储表失败,languages = {},table = {},{}",funcFeatureEngineInfo.language,
                        funcFeatureEngineInfo.curFileTable,e.getMessage());
            } finally {
                unResources(engineComponent);
            }
        }
    }

    private void fullComponent(FuncFeatureEngineInfo engineInfo) {
        List<ComponentCacheInfo> components = null;
        try {
            //获取当前表,指定语言的组件信息
            components = dataBaseJobService.getComponentCount(engineInfo.curFileTable);
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

                dataBaseJobService.updateComponentCacheFlag(component.componentId,0);

                while(engineComponent.isJobRun()) {
                    try {
                        if(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).getPipeLineJobNum()>(pageSize*0.8)){
                            try{
                                TimeUnit.MILLISECONDS.sleep(30L);
                            } catch (Exception e){}
                            continue;
                        }


                        List<Object> jobs = dataBaseJobService.getComponentJobs((curPage-1)*pageSize,pageSize,
                                engineInfo.language,engineInfo.languageTableKeyValue,component);

                        if(CollectionUtils.isEmpty(jobs)){
                            while (engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()) {
                                extractJobService.featureExtract();
                            }
                            break;
                        }

                        engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                        engineComponent.setCurWorkJobCount(jobs.size());


                        while(engineComponent.
                                getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                            extractJobService.featureExtract();
                        }

                        curPage++;
                    } catch (Exception e) {
                        error_log_.info("分页装载任务出错,component.id = {},{}",component.componentId,e.getMessage());
                    }
                }

                dataBaseJobService.updateComponentCacheFlag(component.componentId,1);
            }catch (Exception e) {
                error_log_.info("获取组件信息失败,component.id = {},{}",component.componentId,e.getMessage());
            }finally {
                unResources(engineComponent);
            }
        });
    }
}
