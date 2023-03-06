package com.binsearch.engine;

import com.binsearch.engine.component2file.Engine;
import com.binsearch.etl.EngineComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class BaseEngine {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(Engine.class);

    @Value("${base.service.incremental.running}")
    Boolean incrementalRunning;

    @Value("${base.service.specify.running}")
    Boolean specifyRunning;

    @Value("${base.service.full.running}")
    Boolean fullRunning;

    @Value("#{'${base.service.engine}'.split(',')}")
    List<String> services;

    @Autowired
    Map<String, ETLService> engineMap;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;



    public boolean init(){
        for(String key:services){
            try {
                engineMap.get(key).init();
            }catch(Exception e){
                return false;
            }
        }
        return true;
    }

    /**
     * 停止任务，等待线程释放
     */
    public void stop(){
        engineComponent.unRunJob();//结束任务
        engineComponent.clearCache();//清空队列缓存
    }


    @Scheduled(cron = "0/10 * * * * ?")
    public void startTimestamp(){
        start();
    }



    public void start(){
        if(!engineComponent.isJobRun()){
            engineComponent.runJob(); //任务枷锁

            if(init()){
                if(incrementalRunning){
                    for(String key:services){
                        engineMap.get(key).incrementalComponent();
                    }
                }else if(specifyRunning) {
                    for(String key:services){
                        engineMap.get(key).specifyComponent();
                    }
                    System.exit(1);
                }else if (fullRunning) {
                    for(String key:services){
                        engineMap.get(key).fullComponent();
                    }
                    System.exit(2);
                }
            }else{
                System.exit(3);
            }
            engineComponent.unRunJob(); //任务解锁
        }
        stop();
    }

}
