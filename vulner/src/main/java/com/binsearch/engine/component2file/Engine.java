package com.binsearch.engine.component2file;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.*;
import com.binsearch.etl.EngineComponent;
import com.binsearch.engine.component2file.service.AnalysisJobService;
import com.binsearch.engine.component2file.service.DataBaseJobService;
import com.binsearch.engine.component2file.service.ExtractJobService;

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
 * 组件文件多版本去重
 * 组件压缩包 ==> 组件文件库 ==> 文件表
 * scp /root/Adrill/vulner-1.0-SNAPSHOT.jar root@192.168.1.38:/root/
 * nohup java -jar -Xms10240m -Xmx10240m -Xmn6144  /root/vulner-1.0-SNAPSHOT.jar > /dev/null 2>&1 &
 * ps -ef|grep java
 * nohup /usr/local/jdk-11/bin/java -jar -Xms10240m -Xmx10240m -Xmn6144 /root/vulner-1.0-SNAPSHOT.jar > /dev/null 2>&1 &
 *
 * mount -t nfs -o rw 192.168.1.82:/home/virtual/components/src_code /home/virtual/components/src_code -o proto=tcp
 *
 * tar -zvcf /root/buodo.tar.gz /home/virtual/components/source_unique_base/sourceFile/1091
 * */
public class Engine implements ETLService {

    public final static String SOURCE_TYPE_GIT = "GIT";

    public final static String SOURCE_TYPE_MAVEN = "MAVEN";

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(Engine.class);

    @Autowired
    @Qualifier(EngineConfiguration.COMPONENT2FILE_DATABASE_JOB_SERVICE)
    DataBaseJobService dataBaseJobService;

    @Autowired
    @Qualifier(EngineConfiguration.COMPONENT2FILE_EXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(EngineConfiguration.COMPONENT2FILE_ANALYSIS_JOB_SERVICE)
    AnalysisJobService analysisJobService;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    // 118:[v7.6.0.zip,...]@...格式
    @Value("#{'${base.service.specify.components}'.split('@')}")
    List<String> components;

    @Value("${component2file.target.system.table.count}")
    Integer tableCount;

    public final ConcurrentLinkedQueue<String> cacheQueue = new ConcurrentLinkedQueue<String>();

    @Value("#{'${base.system.source.sourceType}'.split(',')}")
    private List<String> sourceType;


    public void init()throws Exception{
        //初始化表
        dataBaseJobService.creatComponentFile(tableCount);

        //
        if(sourceType.isEmpty()){
            throw new Exception("component2file服务,数据源类型为空!");
        }

        if(!CollectionUtils.isEmpty(components)){
            cacheQueue.addAll(components);
        }
    }

    public void incrementalComponent(){
        cacheQueue.clear();

        for(String temp:sourceType){
            incrementalComponent(temp);
        }
    }

    //增量处理
    private void incrementalComponent(String sourceType){
        int curPage = 1;//当前页数
        int pageSize = 1000;
        int flag = 0;


        while (engineComponent.isJobRun()) {
            try{
                if(cacheQueue.size()>(pageSize*0.8)){
                    try{
                        TimeUnit.MILLISECONDS.sleep(50L);
                    }catch (Exception e){}
                    continue;
                }

                //获取原始组件信息
                List viewTasks = null;
                if(sourceType.equals(SOURCE_TYPE_GIT)){
                    viewTasks = dataBaseJobService.getViewTasks((curPage-1)*pageSize, pageSize);
                }else{
                    viewTasks = dataBaseJobService.getMavenTasks((curPage-1)*pageSize, pageSize);
                }

                if(CollectionUtils.isEmpty(viewTasks)){
                    break;
                }

                //组件版本增量处理
                viewTasks.parallelStream().forEach(viewTask->{
                    String taskId = "",mainLanguage="";
                    try {
                        //获取组件的版本
                        List viewDetailTasks = null;
                        if(sourceType.equals(SOURCE_TYPE_GIT)){
                            viewDetailTasks = dataBaseJobService.getViewDetailTasks((ViewTasks)viewTask);
                        }else{
                            viewDetailTasks = dataBaseJobService.getMavenDetailTasks((MavenTasks)viewTask);
                        }


                        if(CollectionUtils.isEmpty(viewDetailTasks)){
                           return;
                        }

                        if(sourceType.equals(SOURCE_TYPE_GIT)){
                            taskId =  ((ViewTasks)viewTask).taskId;
                            mainLanguage = ((ViewTasks)viewTask).mainLang;

                        }else{
                            taskId = ((MavenTasks) viewTask).compoId;
                            mainLanguage = "JAVA";
                        }

                        //获取缓存
                        ComponentCacheInfo componentCacheInfo = dataBaseJobService.getComponentCache(taskId);

                        if(Objects.isNull(componentCacheInfo)){
                            //增量的组件
                            ComponentCacheInfo cache = new ComponentCacheInfo();
                            cache.setComponentId(taskId);
                            cache.setComponent2fileFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                            cache.setFile2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                            cache.setFunc2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                            cache.setFile2type3featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                            cache.setCodeSource(sourceType);
                            cache.setMainLanguage(mainLanguage);
                            dataBaseJobService.saveComponentCache(cache);
                        }else{
                            //增量的组件版本
                            if(sourceType.equals(SOURCE_TYPE_GIT)){
                                //处理GIT爬取包
                                List<ViewDetailTasks> itmes = new Vector<ViewDetailTasks>(){};

                                viewDetailTasks.forEach(vk ->{
                                    ViewDetailTasks  viewDetailTask = (ViewDetailTasks)vk;
                                    //判断是否已经处理
                                    String versionName = viewDetailTask.versionName;
                                    if(versionName.contains(".zip")){
                                        versionName = versionName.replace(".zip",";");
                                    }else if(versionName.contains(".jar")){
                                        versionName = versionName.replace(".jar",";");
                                    }else{
                                        versionName = versionName+";";
                                    }

                                    if(Strings.isBlank(componentCacheInfo.getComponentVersion())||
                                            !componentCacheInfo.getComponentVersion().contains(versionName)){
                                        itmes.add(viewDetailTask);
                                    }
                                });

                                viewDetailTasks = itmes;
                            }else{
                                //处理Maven爬取包
                                List<MavenDetailTasks> itmes = new Vector<MavenDetailTasks>(){};

                                viewDetailTasks.forEach(vk ->{
                                    MavenDetailTasks  mavenDetailTasks = (MavenDetailTasks)vk;
                                    //判断是否已经处理
                                    String  versionName = mavenDetailTasks.currentVersion+";";
                                    if(versionName.contains(".zip")){
                                        versionName = versionName.replace(".zip",";");
                                    }else if(versionName.contains(".jar")){
                                        versionName = versionName.replace(".jar",";");
                                    }else{
                                        versionName = versionName+";";
                                    }

                                    if(Strings.isBlank(componentCacheInfo.getComponentVersion())||
                                            !componentCacheInfo.getComponentVersion().contains(versionName)){
                                        itmes.add(mavenDetailTasks);
                                    }
                                });

                                viewDetailTasks = itmes;
                            }
                        }

                        //组合增量版本 118:[v7.6.0.zip,...]
                        if(!CollectionUtils.isEmpty(viewDetailTasks)) {
                            String s = "";
                            for (Object viewDetailTask : viewDetailTasks) {
                                if(sourceType.equals(SOURCE_TYPE_GIT)) {
                                    s += ((ViewDetailTasks)viewDetailTask).versionName + ",";
                                }else{
                                    s += ((MavenDetailTasks)viewDetailTask).currentVersion + ",";
                                }
                            }

                            cacheQueue.add(String.format("%s:[%s]", taskId, s.substring(0,s.length()-1)));
                        }
                    }catch (Exception e) {
                        error_log_.info("\n\n ********************* 组件版本增量处理出错 component.id = {}; Exception is {} \n\n",taskId,e.getMessage());
                    }
                });

                specifyComponent(sourceType);

            }catch (Exception e) {
                error_log_.info("\n\n ********************* {}; Exception is {} \n\n",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),e.getMessage());
            }
            curPage++;
        }

        unResources(engineComponent);
    }


    public void specifyComponent(){
        for(String temp:sourceType){
            specifyComponent(temp);
        }
    }

    //
    private void specifyComponent(String sourceType){
        while(true){
            String component = null;
            try {
                component = cacheQueue.poll();
                if(Strings.isBlank(component)){
                    break;
                }

                List<Object> jobs = null;
                if(sourceType.equals(SOURCE_TYPE_GIT)) {
                    jobs = dataBaseJobService.getGitComponentJobs(component);
                }else{
                    jobs = dataBaseJobService.getMavenComponentJobs(component);
                }

                if(CollectionUtils.isEmpty(jobs)){
                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                        extractJobService.fileExtract();
                    }

                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                        analysisJobService.analysisExtract();
                    }
                    continue;
                }

                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                engineComponent.setCurWorkJobCount(jobs.size());


                while(engineComponent.
                        getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                    extractJobService.fileExtract();
                }

                while(engineComponent.
                        getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                    analysisJobService.analysisExtract();
                }
            }catch (Exception e){
                error_log_.info("\n\n ********************* {}; Exception is {} \n\n",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),e.getMessage());
            }finally{
                unResources(engineComponent);
                logger.info("\n\n ********************* {} 本次导入结束 \n\n",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            }
        }
        unResources(engineComponent);
    }


    /**
     * 运行默认任务
     * */
    public void fullComponent(){
        for(String temp:sourceType){
            fullComponent(temp);
        }
    }


    private void fullComponent(String sourceType){
        int count = 0; //总数
        int pageSize = 2;//每页条数
        int pageNum = 0;//页数
        int curPage = 1;//当前页数
        ComponentFileEngineInfo engineInfo = null;

        try{
            engineInfo = dataBaseJobService.getComponenFileEngineInfo();

            if(sourceType.equals(SOURCE_TYPE_GIT)){
                count = dataBaseJobService.getGitComponentCount();
                curPage = engineInfo.curGitPage; //默认1
            }else{
                count = dataBaseJobService.getMavenComponentCount();
                curPage = engineInfo.curMavenPage;
            }

            pageNum = (count-1)/pageSize+1;
        }catch (Exception e){
            error_log_.info("\n\n ********************* {} 引擎初始化失败 \n\n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        }


        while (engineComponent.isJobRun()){
            try {
                if(curPage > pageNum){
                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                        extractJobService.fileExtract();
                    }

                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                        analysisJobService.analysisExtract();
                    }
                    break;
                }

                //
                List<Object> jobs = null;
                if(sourceType.equals(SOURCE_TYPE_GIT)){
                    jobs = dataBaseJobService.getGitComponentJobs((curPage-1)*pageSize,pageSize);
                }else{
                    jobs = dataBaseJobService.getMavenComponentJobs((curPage-1)*pageSize,pageSize);
                }

                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJobs(jobs);
                engineComponent.setCurWorkJobCount(jobs.size());

                while(engineComponent.
                        getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                    extractJobService.fileExtract();
                }

                while(engineComponent.
                        getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).threadNumDecrement()){
                    analysisJobService.analysisExtract();
                }

            }catch (Exception e){
                error_log_.info("\n\n ********************* {};page = {} Exception is {} \n\n",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),String.valueOf(curPage),e.getMessage());
            }finally{
                unResources(engineComponent);

                if(sourceType.equals(SOURCE_TYPE_GIT)){
                    engineInfo.curGitPage = curPage;
                }else{
                    engineInfo.curMavenPage = curPage;
                }

                dataBaseJobService.saveComponenFileEngineInfo(engineInfo);
                curPage++;
                logger.info("\n\n ********************* {} 本次导入结束 countPage = {}, page = {} \n\n",
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),String.valueOf(pageNum),String.valueOf(curPage));
            }
        }
    }
}
