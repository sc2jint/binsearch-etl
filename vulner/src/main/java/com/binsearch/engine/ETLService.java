package com.binsearch.engine;

import com.binsearch.etl.EngineComponent;

public interface ETLService {

    public void init()throws Exception;


    /**
     * 获取增量
     */
    public void incrementalComponent();

    /**
     * 获取配置指定
     */
    public void specifyComponent();


    /**
     * 获取全量
     */
    public void fullComponent();


    default public void unResources(EngineComponent<Object>  engineComponent){
        while (true){
            if (engineComponent.isThreadClose()){
                break;
            }
            try {
                Thread.sleep(200L);
            } catch (Exception e) {}
        }
    }

}

