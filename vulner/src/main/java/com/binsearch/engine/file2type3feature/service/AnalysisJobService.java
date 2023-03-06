package com.binsearch.engine.file2type3feature.service;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.FileType3Feature;
import com.binsearch.engine.file2type3feature.FileType3FeatureDTO;
import com.binsearch.engine.file2type3feature.WorkJob;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.orm.JdbcUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
public class AnalysisJobService {

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;


    /**
     * 重复检数据查获取异步任务
     */
    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void analysisExtract(){

        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS);
        try {
            while (engineComponent.isJobRun()) {
                Object job = pipeLineComponent.getPipeLineJobs();
                if (Objects.isNull(job)) {
                    if (engineComponent.getCurWorkJobCount() == 0) {
                        break;
                    } else {
                        try {
                            TimeUnit.MILLISECONDS.sleep(200);
                        } catch (Exception e) {
                        }
                        continue;
                    }
                }
                analysisExtract((WorkJob) job);
            }
        }finally {
            pipeLineComponent.threadNumIncrement();
        }
    }

    /**
     * 组件关系数据提取
     * */
    public void analysisExtract(WorkJob job){
        job.getTask().log("///////////////// AnalysisJobService /////////////////");
        try{
            ConcurrentMap<Long, FileType3FeatureDTO> concurrentMap = job.getTask().getLoadingCache().asMap();
            if(CollectionUtils.isEmpty(concurrentMap)){
                throw new Exception(String.format("******  Exception component.id = %s;没有可同步的文件",job.getTask().getComponentCacheInfo().componentId));
            }

            int dataInsertNum = 0;
            ArrayList<FileType3Feature> temps = new ArrayList<FileType3Feature>();
            for(FileType3FeatureDTO fileFeatureDTO:concurrentMap.values()){
                if(Objects.isNull(fileFeatureDTO)) continue ;

                if (fileFeatureDTO.getDataBaseFlag() == FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_) {
                    synchronized (fileFeatureDTO) {
                        if (fileFeatureDTO.getDataBaseFlag() == FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_) {
                            fileFeatureDTO.setDataBaseFlag(FileType3FeatureDTO.DATE_BASE_FLAG_SAVE_);
                            dataInsertNum++;
                            temps.add(fileFeatureDTO.getFileType3Feature());

                            if (temps.size() == 200) {
                                createFileType3Feature(job, temps);
                                temps.clear();
                            }
                        }
                    }
                }
            }
            if(!CollectionUtils.isEmpty(temps)){
                createFileType3Feature(job,temps);
            }
            job.getTask().log(String.format("****** 同步数据量 dataInsertNum = %s ; component.id = %s",dataInsertNum,job.getTask().getComponentCacheInfo().componentId));
        }catch (Exception e){
            job.getTask().errorLog(e.getMessage());
        }finally {
            engineComponent.curWorkJobCountDecrement();
            job.getTask().log("///////////////// job close /////////////////");
            job.getTask().logPrintln();
        }
    }

    private void createFileType3Feature(WorkJob job, List<FileType3Feature> fileType3Features){
        try {
            Exception exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                     .table(job.getTask().getCurFileType3FeatureTable()).batchCreate(fileType3Features.toArray()).error();
            if (Objects.nonNull(exception)) {
                throw exception;
            }
        }catch (Exception e){
            job.getTask().errorLog(
                    String.format("******  Exception 持久化数据失败,component.id = %s;%s",
                    job.getComponentFile().componentId,e.getMessage()));
        }
    }

}
