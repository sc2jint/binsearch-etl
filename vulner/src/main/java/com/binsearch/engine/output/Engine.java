package com.binsearch.engine.output;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.db.EngineRunningError;
import com.binsearch.engine.output.service.ExtractJobService;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.binsearch.engine.output.EngineConfiguration.OUTPUTSQL_ENGINE;
import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TEMPLATE;
import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TRANSACTION_TEMPLATE;

public class Engine  implements ETLService {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(com.binsearch.engine.component2file.Engine.class);

    @Autowired
    @Qualifier(ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    @Qualifier(ETL_JDBC_TRANSACTION_TEMPLATE)
    public DataSourceTransactionManager etlDataSourceTransactionManager;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;


    @Autowired
    @Qualifier(EngineConfiguration.OUTPUTSQL_EXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;


    @Value("#{'${base.service.specify.components}'.split('@')}")
    List<String> components;

    public final ConcurrentLinkedQueue<String> cacheQueue = new ConcurrentLinkedQueue<String>();

    @Override
    public void init() throws Exception {
        if(!CollectionUtils.isEmpty(components)){
            cacheQueue.addAll(components);
        }
    }


    public void saveOutPutErrorLog(String id,String log,String target)throws Exception{
        EngineRunningError error = new EngineRunningError();
        error.setComponentId(id);
        error.setErrorDate(new Timestamp(System.currentTimeMillis()));
        error.setRunningErrorInfo(log);
        error.setTargetTable(target);
        error.setEngineType(OUTPUTSQL_ENGINE);

        Exception exception = new JdbcUtils(etlJdbcTemplate).create(error).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
    }

    @Override
    public void incrementalComponent() {}

    @Override
    public void specifyComponent()  {
        while(true) {
            String componentId = null;
            try {
                componentId = cacheQueue.poll();
                if (Strings.isBlank(componentId)) {
                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                        extractJobService.featureExtract();
                    }
                    break;
                }

                List<ComponentCacheInfo> caches = new ArrayList<>();
                Exception exception = new JdbcUtils(etlJdbcTemplate).model(ComponentCacheInfo.class)
                        .where("component_id = ?",componentId)
                        .query(caches).error();


                if(CollectionUtils.isEmpty(caches) || Objects.nonNull(exception)){
                    continue;
                }

                for(ComponentCacheInfo cache:caches) {
                    WorkJob workJob = new WorkJob();
                    workJob.setCacheInfo(cache);
                    engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJob(workJob);
                }

                engineComponent.setCurWorkJobCount(caches.size());

                while(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                    extractJobService.featureExtract();
                }

            } catch (Exception e) {
                try {
                    saveOutPutErrorLog(componentId, "获取指定导出组件出错:" + e.getMessage(), "");
                }catch (Exception ex) {}
            }
        }
        unResources(engineComponent);
    }

    public int getComponentCacheInfoCount(){
        EntityCountNum num = new EntityCountNum();
        new JdbcUtils(etlJdbcTemplate).model(ComponentCacheInfo.class).count(num);
        return num.getCountNum();
    }

    @Override
    public void fullComponent() {
        int pageSize = 1000;//页数
        int curPage = 1;//当前页数

        int count  = getComponentCacheInfoCount();

        while (engineComponent.isJobRun()){
            try {
                List<ComponentCacheInfo> caches = new ArrayList<>();

                Exception exception = new JdbcUtils(etlJdbcTemplate).model(ComponentCacheInfo.class)
                        .where("id > ? and id <= ?",(curPage-1) * pageSize,curPage * pageSize)
                        .query(caches).error();


                if( ((curPage-1) * pageSize) > count || Objects.nonNull(exception)){
                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                        extractJobService.featureExtract();
                    }
                    break;
                }


                for(ComponentCacheInfo cache:caches) {
                    WorkJob workJob = new WorkJob();
                    workJob.setCacheInfo(cache);
                    engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJob(workJob);
                }

                engineComponent.setCurWorkJobCount(caches.size());

                while(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                    extractJobService.featureExtract();
                }

            }catch (Exception e){}
            finally{
                unResources(engineComponent);
                curPage++;
            }
        }
        unResources(engineComponent);
    }
}
