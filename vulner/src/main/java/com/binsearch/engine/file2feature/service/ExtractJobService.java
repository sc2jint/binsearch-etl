package com.binsearch.engine.file2feature.service;


import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.engine.entity.db.FileFeature;
import com.binsearch.engine.file2feature.Task;
import com.binsearch.engine.file2feature.WorkJob;
import com.binsearch.engine.file2feature.FileFeatureDTO;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.ftp.FtpObjectPool;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import com.ubisec.UniSCAFossRadar.Preprocessor.Common.Constants.LanguageCode;
import com.ubisec.UniSCAFossRadar.Preprocessor.Feature.CodeFeature;
import com.ubisec.UniSCAFossRadar.Preprocessor.SimplePreprocessor;
import com.ubisec.UniSCAFossRadar.Preprocessor.SimplePreprocessorContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
public class ExtractJobService {

    @Value("${base.system.targetFileRootPath}")
    public String sourceRootPath;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;


    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;

    @Value("${base.cache.maximum}")
    long cacheMaximum;

    @Value("${file2feature.source.system.sftp.file.path}")
    String filePath;

    //目标表数
    @Value("${file2feature.target.system.table.count}")
    Integer tableCount;

    @Value("${file2feature.target.system.data.count}")
    Integer dataCount;


    @Autowired
    @Qualifier(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public Executor simpleAsync;

    //是否开启ftp
    @Value("${file2feature.source.system.sftp.enable}")
    Boolean sFtpEnable;

    @Value("#{'${file2feature.source.system.sftp.connection}'.split(':')}")
    List<String> sFtpInfos;

    FtpObjectPool ftpObjectPool;

    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void featureExtract() {
        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS);

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
                featureExtract((WorkJob) job);
            }
        }catch (Exception e){}
        finally {
            pipeLineComponent.threadNumIncrement();
        }
    }


    private void featureExtract(WorkJob job) {
        try {
            if(Objects.isNull(job.getTask().getLoadingCache())) {
                synchronized (job.getTask()) {
                    if (Objects.isNull(job.getTask().getLoadingCache())) {
                        try {
                            getFileFeatureTable(job.getTask());

                            //实例化缓存
                            job.getTask().initLoadingCache(cacheMaximum, simpleAsync, new RemovalListener<Long, FileFeatureDTO>() {
                                @Override
                                public void onRemoval(@Nullable Long key, @Nullable FileFeatureDTO value, RemovalCause cause) {
                                    try {
                                        if (Objects.isNull(value)) return;

                                         while (true) {
                                             if (value.getCacheFlag() != FileFeatureDTO.CACHE_FLAG_SAVE_) {
                                                 try {
                                                     TimeUnit.MILLISECONDS.sleep(100L);
                                                 } catch (Exception e) {}
                                                 continue;
                                             }

                                             if (value.getDataBaseFlag() == FileFeatureDTO.DATE_BASE_FLAG_UN_SAVE_){
                                                 synchronized (value) {
                                                     if (value.getDataBaseFlag() == FileFeatureDTO.DATE_BASE_FLAG_UN_SAVE_) {
                                                         value.setDataBaseFlag(FileFeatureDTO.DATE_BASE_FLAG_SAVE_);
                                                         Exception exception = null;
                                                         if (Objects.isNull(value.getFileFeature().getId()) || value.getFileFeature().getId() == 0) {
                                                             exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                                                                     .table(value.getCurFileFeatureTable()).create(value.getFileFeature()).error();
                                                         }
                                                         if (Objects.nonNull(exception)) {
                                                             value.setDataBaseFlag(FileFeatureDTO.DATE_BASE_FLAG_UN_SAVE_);
                                                             throw exception;
                                                         }
                                                     }
                                                 }
                                             }
                                             break;
                                        }
                                    } catch (Exception e) {
                                        job.getTask().errorLog(String.format("******  Exception 缓存清除事件失败,component.id = %s; table = %s; component_file_id = %s; %s",
                                                value.getFileFeature().getComponentId(), value.getCurFileFeatureTable(), value.getFileFeature().getComponentFileId(), e.getMessage()));
                                    }
                                }
                            });

                            //初始化组件缓存
                            if (job.getTask().getComponentCacheInfo().getNum() > 0) {
                                List<FileFeature> fileFeatures = getFileFeatures(job.getTask().getComponentCacheInfo().componentId, job.getTask().getCurFileFeatureTable());

                                //初始化缓存
                                for (FileFeature fileFeature : fileFeatures) {
                                    job.getTask().getLoadingCache().put(
                                            fileFeature.componentFileId,
                                            new FileFeatureDTO(
                                                    fileFeature,
                                                    job.getTask().getCurFileFeatureTable(),
                                                    FileFeatureDTO.DATE_BASE_FLAG_SAVE_,
                                                    FileFeatureDTO.CACHE_FLAG_SAVE_));
                                }
                                job.getTask().log(String.format("******  initCacheSize = %s,component.id= %s", fileFeatures.size(), job.getTask().getComponentCacheInfo().componentId));
                            }
                        }catch (Exception initException){
                            job.getTask().log(String.format("******  初始化失败 component.id = %s , msg = %s ",job.getTask().getComponentCacheInfo().componentId,initException.getMessage()));
                            return;
                        }
                    }
                }
            }

            FileFeatureDTO dto = getCacheFileFeatureDTO(job);
            if(Objects.nonNull(dto)){
                System.out.println(String.format("component.id = %s,componentFile.id= %s 已存在",dto.getFileFeature().componentId,dto.getFileFeature().componentFileId));
                return;
            }

            //获取文件路径
            ComponentFile componentFile = job.getComponentFile();
            String sourceFilePath = String.format("%s%s",sourceRootPath,componentFile.getFilePath());

            String targetFile = filePath;
            targetFile = !targetFile.endsWith(File.separator) ? targetFile + File.separator : targetFile;
            targetFile = targetFile + componentFile.getFilePath().replace("/","\\");


            // 确定源文件类型,文本,路径
            String text = "";
            LanguageCode languageCode = null;

            if(job.getTask().getLanguage().equalsIgnoreCase("JAVA")){
                languageCode = LanguageCode.JAVA;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getTask().getLanguage().equals("GOLANG")){
                languageCode = LanguageCode.GO;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getTask().getLanguage().equals("C")){
                languageCode = LanguageCode.C;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getTask().getLanguage().equals("C++")){
                languageCode = LanguageCode.CPP;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getTask().getLanguage().equals("PYTHON")){
                languageCode = LanguageCode.PYTHON;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }

            if(Strings.isBlank(text)){
                job.getTask().errorLog(String.format("component.id= %s ,path= %s ,解析文件文本为空",componentFile.getComponentId(),sourceFilePath));
                return;
            }

            //解析源文件
            SimplePreprocessorContext context = new SimplePreprocessorContext();
            context.addPreprocessorContextType(SimplePreprocessorContext.Type.Type2File);
            CodeFeature codeFeature = runningCodeTask(new CodeTask(new SimplePreprocessor(context),text,languageCode),job);

            if(Objects.isNull(codeFeature)){
                job.getTask().errorLog(String.format("component.id= %s 解析文本失败",componentFile.getComponentId()));
                return;
            }

            //封装数据
            FileFeature feature = new FileFeature();
            feature.setComponentFileId(componentFile.getId());
            feature.setComponentId(componentFile.getComponentId());
            feature.setType0(codeFeature.getFeature(CodeFeature.FeatureAttribute.TYPE0).toString());
            feature.setType1(codeFeature.getFeature(CodeFeature.FeatureAttribute.TYPE1).toString());
            feature.setType2blind(codeFeature.getFeature(CodeFeature.FeatureAttribute.TYPE2Blind).toString());
            feature.setSourceTable(job.getTask().getComponentCacheInfo().getComponentFileName());
            feature.setCreateDate(new Timestamp(System.currentTimeMillis()));
            feature.setLineNumber(
                    (Integer)codeFeature.getFeature(CodeFeature.FeatureAttribute.LINENUMBER).getFeature());

            //封装cache对象
            dto = new FileFeatureDTO();
            dto.setCacheFlag(FileFeatureDTO.CACHE_FLAG_UN_SAVE_);
            dto.setCurFileFeatureTable(job.getTask().getCurFileFeatureTable());
            dto.setDataBaseFlag(FileFeatureDTO.DATE_BASE_FLAG_UN_SAVE_);
            dto.setFileFeature(feature);
            job.getTask().getLoadingCache().put(dto.getFileFeature().componentFileId, dto);
            dto.setCacheFlag(FileFeatureDTO.CACHE_FLAG_SAVE_);

        } catch (Exception e) {
            job.getTask().errorLog(String.format("****** Exception 组件分析出错; component.id = %s; component_file_id = %s msg %s",
                    job.getTask().getComponentCacheInfo().componentId,job.getComponentFile().id,e.getMessage()));
        }finally {
            job.getTask().workJobCountDecrement();
            if(job.getTask().getWorkJobCount() == 0) {
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).addPipeLineJob(job);
            }else{
                engineComponent.curWorkJobCountDecrement();
            }
        }
    }

    //异步分析文件
    private CodeFeature runningCodeTask(CodeTask task,WorkJob job){
        CodeFeature codeFeature = null;
        FutureTask<CodeFeature> futureTask = null;
        Thread thread = null;

        try {
            futureTask = new FutureTask<CodeFeature>(task);
            thread = new Thread(futureTask);
            thread.start();
            codeFeature = futureTask.get(60L,TimeUnit.SECONDS);
        }catch (Exception e){
            if(Objects.nonNull(futureTask)){
                futureTask.cancel(true);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(200L);
            }catch (Exception sl){}

            if(Objects.nonNull(thread) && !thread.isInterrupted()){
                thread.interrupt();
            }
            job.getTask().errorLog(String.format("****** componentFile 分析错误; component.id = %s; component_file_id = %s; filepath = %s msg %s",
                    job.getTask().getComponentCacheInfo().componentId,job.getComponentFile().id,job.getComponentFile().getFilePath(),e.getMessage()));
        }
        return codeFeature;
    }


    private List<FileFeature> getFileFeatures(String componentId,String fileTable)throws Exception{
        List<FileFeature> fileFeatures = new ArrayList<FileFeature>(){};
        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(FileFeature.class)
                .table(fileTable)
                .where("component_id = ?",componentId)
                .limit(cacheMaximum)
                .query(fileFeatures)
                .error();
        if(Objects.nonNull(exception)){
            throw exception;
        }
        return fileFeatures;
    }

    private FileFeatureDTO getCacheFileFeatureDTO(WorkJob job){
        return job.getTask().getLoadingCache()
            .get(job.getComponentFile().getId(), new Function<Long, FileFeatureDTO>() {
                @Override
                public FileFeatureDTO apply(Long componentFileId) {

                    ArrayList<FileFeature> temps = new ArrayList<FileFeature>(){};
                    Exception exception = new JdbcUtils(jdbcTemplate).query(
                            String.format("select * from %s where component_id = ? and component_file_id = ?",job.getTask().getCurFileFeatureTable()),
                            FileFeature.class,
                            temps,
                            job.getTask().getComponentCacheInfo().componentId,componentFileId).error();

                    if (Objects.nonNull(exception)){
                        job.getTask().errorLog(String.format("******  Exception 缓存加载数据失败,table = %s; component_id= %s; component_file_id= %s",
                                job.getTask().getCurFileFeatureTable(),job.getTask().getComponentCacheInfo().componentId,componentFileId));
                        return null;
                    }

                    return temps.size()==0 ?
                        null: new FileFeatureDTO(temps.get(0),
                        job.getTask().getCurFileFeatureTable(),
                        FileFeatureDTO.DATE_BASE_FLAG_SAVE_,
                        FileFeatureDTO.CACHE_FLAG_SAVE_);
                }
            });
    }


    private void getFileFeatureTable(Task task)throws Exception{
        String language = task.getLanguageTableKeyValue().toLowerCase();

        char[] chars = language.toCharArray();

        String fileFeatures = String.format("getSourcecode%s%sFilefeatureName",
                String.valueOf(chars[0]).toUpperCase(),
                chars.length==1?"":String.valueOf(ArrayUtils.subarray(chars,1,chars.length)));

        Method method = ComponentCacheInfo.class.getMethod(fileFeatures);
        Object fileFeaturesName = method.invoke(task.getComponentCacheInfo());

        if(Objects.nonNull(fileFeaturesName)){
            task.setCurFileFeatureTable(fileFeaturesName.toString());
            task.getComponentCacheInfo().setNum(1);
            return;
        }


        //检查相关表数据数据量
        String tableName = "";
        for(int i = tableCount;i > 0 ;i--){
            tableName = String.format("t_sourcecode_%s_filefeature%d",language,i);
            EntityCountNum num = new EntityCountNum();
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .table(tableName).count(num).error();

            if (Objects.nonNull(exception)){
                throw exception;
            }

            if(num.getCountNum() == 0){
                continue;
            }

            if(num.getCountNum() < dataCount){
                break;
            }

            if(num.getCountNum() >= dataCount){
                i++;
                tableName = String.format("t_sourcecode_%s_filefeature%d",language,i);
                break;
            }
        }

        task.setCurFileFeatureTable(tableName);
        task.getComponentCacheInfo().setNum(0);
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",task.getComponentCacheInfo().componentId)
                .update("sourcecode_"+language+"_filefeature_name = ?",tableName).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class CodeTask  implements Callable<CodeFeature> {
        SimplePreprocessor preprocessor;

        String text;

        LanguageCode languageCode;


        @Override
        public CodeFeature call() throws Exception {
            CodeFeature codeFeature = null;
            if (Objects.nonNull(preprocessor)) {
                try {
                    codeFeature = preprocessor.execute(text, languageCode);
                } finally {
                    preprocessor.close();
                }
            }
            return codeFeature;
        }
    }
}
