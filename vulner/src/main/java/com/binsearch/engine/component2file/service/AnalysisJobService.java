package com.binsearch.engine.component2file.service;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.component2file.ComponentFileDTO;
import com.binsearch.engine.component2file.WorkJob;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.ftp.FtpObjectPool;
import com.binsearch.etl.orm.JdbcUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.binsearch.etl.ETLConfiguration.ETL_BASE_THREAD_POOL;

public class AnalysisJobService {

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;


    @Value("${component2file.target.system.table.count}")
    Integer tableCount;

    @Value("${base.system.targetFileRootPath}")
    private String targetFileRootPath;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;


    FtpObjectPool ftpObjectPool;

    @Value("#{'${component2file.source.system.sftp.connection}'.split(':')}")
    List<String> sFtpInfos;

    @Value("${component2file.source.system.sftp.enable}")
    Boolean sFtpEnable;

    /**
     * 重复检数据查获取异步任务
     */
    @Async(ETL_BASE_THREAD_POOL)
    public void analysisExtract(){

        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS);
        try{
            while(engineComponent.isJobRun()){
                Object job = pipeLineComponent.getPipeLineJobs();
                if(Objects.isNull(job)){
                    if(engineComponent.getCurWorkJobCount() == 0){
                        break;
                    }else{
                        try {
                            TimeUnit.MILLISECONDS.sleep(200);
                        }catch (Exception e){}
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
            synCache(job);
        }catch (Exception e){
            job.errorLog(e.getMessage());
        }finally {
//          更新组件缓存状态
            Exception exception = new JdbcUtils(etlJdbcTemplate)
                   .model(ComponentCacheInfo.class)
                   .where("component_id = ?", job.getTaskId())
                   .update("component2file_flag = ?", 1).error();

            if(Objects.nonNull(exception)){
                job.errorLog(String.format("****** 更新组件缓存状态失败 componentId = %s;%s",job.getTaskId(),exception.getMessage()));
            }

            engineComponent.curWorkJobCountDecrement();
            job.getTask().log("///////////////// job close /////////////////");
            job.getTask().logPrintln();
        }
    }

    public void synCache(WorkJob job)throws Exception{
        if(Objects.isNull(ftpObjectPool)) {
            synchronized (this) {
                if(Objects.isNull(ftpObjectPool)){
                    FtpObjectPool.FtpProp ftpProp = new FtpObjectPool.FtpProp(
                            sFtpInfos.get(0),sFtpInfos.get(1),
                            sFtpInfos.get(2),sFtpInfos.get(3),sFtpEnable);

                    ftpObjectPool = new FtpObjectPool(ftpProp).init();
                }
            }
        }


        ConcurrentMap<String, ComponentFileDTO>  concurrentMap = job.getTask().getLoadingCache().asMap();
        if(CollectionUtils.isEmpty(concurrentMap)){
            throw new Exception(String.format("******  Exception component.id = %s;versionName = %s;没有可同步的文件",job.getTaskId(),job.getVersionName()) );
        }

        job.getTask().log(String.format("****** 同步文件总量 %s",concurrentMap.size()));

        int fileNum = 0;
        int dataUpdateNum = 0;
        int dataInsertNum = 0;

        for(ComponentFileDTO componentFileDTO:concurrentMap.values()){
            try {
                if(Objects.isNull(componentFileDTO)) continue ;
                synchronized (componentFileDTO){
                    if(componentFileDTO.getSystemFileFlag() == ComponentFileDTO.SYSTEM_FILE_FLAG_UN_SAVE_ &&
                            Objects.nonNull(componentFileDTO.getFileBytes()) && componentFileDTO.getFileBytes().length > 0 ){

                        String stringBuilder = String.format("%s/%s",targetFileRootPath,componentFileDTO.getComponentFile().getFilePath());
                        stringBuilder =  stringBuilder.replace("//","/");
                        if (!ftpObjectPool.isFileExist(stringBuilder)) {
                            ftpObjectPool.upload(componentFileDTO.getFileBytes(),stringBuilder);
                            fileNum++;
                        }

                        componentFileDTO.setSystemFileFlag(ComponentFileDTO.SYSTEM_FILE_FLAG_SAVE_);
                        componentFileDTO.setFileBytes(null);
                    }

                    if(componentFileDTO.getDataBaseFlag() == ComponentFileDTO.DATE_BASE_FLAG_UN_SAVE_) {
                        Exception exception = null;

                        if (Objects.nonNull(componentFileDTO.getComponentFile().getId()) &&
                                componentFileDTO.getComponentFile().getId() > 0) {
                            exception = new JdbcUtils(jdbcTemplate, transactionTemplate).table(job.getTask().getComponentCacheInfo().getComponentFileName())
                                    .where("component_id = ? and file_hash_value = ?", job.getTaskId(), componentFileDTO.getComponentFile().getFileHashValue())
                                    .update("source_file_path = ?,component_version_id = ?", componentFileDTO.getComponentFile().getSourceFilePath(), componentFileDTO.getComponentFile().getComponentVersionId()).error();
                            dataUpdateNum++;
                        } else {
                            exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                                    .table(job.getTask().getComponentCacheInfo().getComponentFileName()).create(componentFileDTO.getComponentFile()).error();
                            dataInsertNum++;
                        }

                        if (Objects.nonNull(exception)) {
                            throw exception;
                        }

                        componentFileDTO.setDataBaseFlag(ComponentFileDTO.DATE_BASE_FLAG_SAVE_);
                    }
                }
            }catch (Exception e){
                job.errorLog(String.format("******  Exception 持久化数据失败,component.id = %s; filePath = %s;%s",
                        job.getTaskId(),componentFileDTO.getComponentFile().getFilePath(),e.getMessage()));
            }
        }

        job.getTask().log(String.format("****** 同步数据量 file.size = %s; dataUpdateNum = %s; dataInsertNum = %s",fileNum,dataUpdateNum,dataInsertNum));
    }
}