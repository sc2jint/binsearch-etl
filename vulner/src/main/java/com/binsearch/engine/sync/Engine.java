package com.binsearch.engine.sync;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Engine implements ETLService {
    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(Engine.class);

    @Autowired
    @Qualifier(EngineConfiguration.SYNC_EXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate targetJdbctemplat;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate targetTransactionTemplate;

    public void init()throws Exception{
        //初始化表

    }

    //增量处理
    public void incrementalComponent() {

    }

    //
    public void specifyComponent() {

    }


    /**
     * 运行默认任务
     * */
    public void fullComponent() {
        for(int i = 10;i<=25;i++){
            Class clazz = ComponentFile.class;
            String tableName = String.format("t_component_file%d",i);
            int curPage = 1;//当前页数
            int pageSize = 100;//每页条数

            EntityCountNum num = new EntityCountNum();
            Exception exception = new JdbcUtils(targetJdbctemplat)
                .table(tableName).count(num).error();

            if(Objects.nonNull(exception)){
                return;
            }

            while (true) {
                try {
                    if(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).getPipeLineJobNum()>800){
                        try{
                            TimeUnit.MILLISECONDS.sleep(10L);
                        }catch (Exception e){}
                        continue;
                    }

                    int s = (curPage - 1) * pageSize;
                    int e = curPage*pageSize;

                    ArrayList<Object> componentFiles = new ArrayList<Object>();
                    exception = new JdbcUtils(targetJdbctemplat)
                            .model(clazz).table(tableName)
                            .where("id > ? and id <= ? ", s,e)
                            .query(componentFiles).error();

                    if (Objects.nonNull(exception)) {
                        throw exception;
                    }

                    curPage++;

                    if(CollectionUtils.isEmpty(componentFiles)){
                        if(s > num.getCountNum()){
                            while(engineComponent.
                                    getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                                extractJobService.fileExtract();
                            }
                            break;
                        }
                        continue;
                    }

                    WorkJob workJob = new WorkJob();
                    workJob.setEntitys(componentFiles);
                    workJob.setTableName(tableName);
                    workJob.setClazz(clazz);

                    engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJob(workJob);

                    while(engineComponent.
                            getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                        extractJobService.fileExtract();
                    }

                }catch (Exception e) {
                    System.out.println(e.getMessage());
                    error_log_.info(e.getMessage());
                }
            }

            unResources(engineComponent);
        }
    }
}

