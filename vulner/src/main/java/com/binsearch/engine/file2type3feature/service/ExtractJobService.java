package com.binsearch.engine.file2type3feature.service;


import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.engine.entity.db.FileType3Feature;
import com.binsearch.engine.file2type3feature.FileType3FeatureDTO;
import com.binsearch.engine.file2type3feature.Task;
import com.binsearch.engine.file2type3feature.WorkJob;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.ftp.FtpObjectPool;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.ubisec.UniSCAFossRadar.Preprocessor.*;
import com.ubisec.UniSCAFossRadar.Preprocessor.Common.Constants.*;
import com.ubisec.UniSCAFossRadar.Preprocessor.Feature.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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


    //目标表数
    @Value("${file2type3feature.target.system.table.count}")
    Integer tableCount;

    @Value("${file2type3feature.target.system.data.count}")
    Integer dataCount;


    @Autowired
    @Qualifier(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public Executor simpleAsync;

    //是否开启ftp
    @Value("${file2type3feature.source.system.sftp.enable}")
    Boolean sFtpEnable;

    @Value("#{'${file2type3feature.source.system.sftp.connection}'.split(':')}")
    List<String> sFtpInfos;

    @Value("${file2type3feature.source.system.sftp.file.path}")
    String filePath;

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
                            job.getTask().initLoadingCache(cacheMaximum, simpleAsync, new RemovalListener<Long, FileType3FeatureDTO>() {
                                @Override
                                public void onRemoval(@Nullable Long key, @Nullable FileType3FeatureDTO value, RemovalCause cause) {
                                    try {
                                        if (Objects.isNull(value)) return;

                                         while (true) {
                                             if (value.getCacheFlag() != FileType3FeatureDTO.CACHE_FLAG_SAVE_) {
                                                 try {
                                                     TimeUnit.MILLISECONDS.sleep(100L);
                                                 } catch (Exception e) {
                                                 }
                                                 continue;
                                             }

                                             if (value.getDataBaseFlag() == FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_){
                                                 synchronized (value) {
                                                     if (value.getDataBaseFlag() == FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_) {
                                                         value.setDataBaseFlag(FileType3FeatureDTO.DATE_BASE_FLAG_SAVE_);
                                                         Exception exception = null;
                                                         if (Objects.isNull(value.getFileType3Feature().getId()) || value.getFileType3Feature().getId() == 0) {
                                                             exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                                                                     .table(value.getCurFileType3FeatureTable()).create(value.getFileType3Feature()).error();
                                                         }
                                                         if (Objects.nonNull(exception)) {
                                                             value.setDataBaseFlag(FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_);
                                                             throw exception;
                                                         }
                                                     }
                                                 }
                                             }
                                             break;
                                        }
                                    } catch (Exception e) {
                                        job.getTask().errorLog(String.format("******  Exception 缓存清除事件失败,component.id = %s; table = %s; component_file_id = %s; %s",
                                                value.getFileType3Feature().getComponentId(), value.getCurFileType3FeatureTable(), value.getFileType3Feature().getComponentFileId(), e.getMessage()));
                                    }
                                }
                            });

                            //初始化组件缓存
                            if (job.getTask().getComponentCacheInfo().getNum() > 0) {
                                List<FileType3Feature> fileType3Features = getFileType3Feature(job.getTask().getComponentCacheInfo().componentId, job.getTask().getCurFileType3FeatureTable());

                                //初始化缓存
                                for (FileType3Feature fileType3Feature : fileType3Features) {
                                    job.getTask().getLoadingCache().put(
                                            fileType3Feature.componentFileId,
                                            new FileType3FeatureDTO(
                                                    fileType3Feature,
                                                    job.getTask().getCurFileType3FeatureTable(),
                                                    FileType3FeatureDTO.DATE_BASE_FLAG_SAVE_,
                                                    FileType3FeatureDTO.CACHE_FLAG_SAVE_));
                                }
                                job.getTask().log(String.format("******  initCacheSize = %s,component.id= %s", fileType3Features.size(), job.getTask().getComponentCacheInfo().componentId));
                            }
                        }catch (Exception initException){
                            job.getTask().log(String.format("******  初始化失败 component.id = %s , msg = %s ",job.getTask().getComponentCacheInfo().componentId,initException.getMessage()));
                            return;
                        }
                    }
                }
            }


            FileType3FeatureDTO dto = getCacheFileFeatureDTO(job);
            if(Objects.nonNull(dto)){
                System.out.println(String.format("component.id = %s,componentFile.id= %s 已存在",dto.getFileType3Feature().componentId,dto.getFileType3Feature().componentFileId));
                return;
            }

            ComponentFile componentFile = job.getComponentFile();
            //解析文件
            String text = "";
            String sourceFilePath = String.format("%s%s",sourceRootPath,componentFile.getFilePath());
            String targetFile = filePath;

            //判断目标文件后坠
            targetFile = !targetFile.endsWith(File.separator) ? targetFile + File.separator : targetFile;
            targetFile = targetFile + componentFile.getFilePath().replace("/","\\");

            //获取文本
            text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);

            if(Strings.isBlank(text)){
                job.getTask().errorLog(String.format("component.id= %s ,path= %s ,解析文件文本为空",componentFile.getComponentId(),sourceFilePath));
                return;
            }

            PreprocessorContext context = new PreprocessorContext();
            context.languages.add(LanguageCode.JAVA);
            context.types.add(PreprocessorContext.Type.Type3MinHash);
            Preprocessor preprocessor = new Preprocessor(context);
            CodeFeature codeFeature = preprocessor.execute(text,LanguageCode.JAVA);

            if(Objects.isNull(codeFeature)){
                job.getTask().errorLog(String.format("component.id= %s 解析文本失败",componentFile.getComponentId()));
                return;
            }

            ArrayHashFeature minHash = (ArrayHashFeature) codeFeature.getFeature(CodeFeature.FeatureAttribute.TYPE3MINHASH);

            //封装数据
            List<BigInteger> minHashValues = (List<BigInteger>) minHash.getFeature();

            FileType3Feature feature = new FileType3Feature();
            feature.setComponentFileId(componentFile.getId());
            feature.setComponentId(componentFile.getComponentId());
            feature.setTypes(StringUtils.join(minHashValues,","));
            feature.setSourceTable(job.getTask().getComponentCacheInfo().getComponentFileName());
            feature.setCreateDate(new Timestamp(System.currentTimeMillis()));

            for(int i = 1;i <= 60;i++){
                Method method = FileType3Feature.class.getMethod(String.format("setType%d",i),String.class);
                method.invoke(feature,minHashValues.get(i-1).toString());
            }

            //封装cache对象
            dto = new FileType3FeatureDTO();
            dto.setCacheFlag(FileType3FeatureDTO.CACHE_FLAG_UN_SAVE_);
            dto.setCurFileType3FeatureTable(job.getTask().getCurFileType3FeatureTable());
            dto.setDataBaseFlag(FileType3FeatureDTO.DATE_BASE_FLAG_UN_SAVE_);
            dto.setFileType3Feature(feature);
            job.getTask().getLoadingCache().put(dto.getFileType3Feature().componentFileId, dto);
            dto.setCacheFlag(FileType3FeatureDTO.CACHE_FLAG_SAVE_);
            System.out.println(" ====================================== ");
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


    private List<FileType3Feature> getFileType3Feature(String componentId, String fileTable)throws Exception{
        List<FileType3Feature> fileFeatures = new ArrayList<FileType3Feature>(){};
        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(FileType3Feature.class)
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

    private FileType3FeatureDTO getCacheFileFeatureDTO(WorkJob job){
        return job.getTask().getLoadingCache()
            .get(job.getComponentFile().getId(), new Function<Long, FileType3FeatureDTO>() {
                @Override
                public FileType3FeatureDTO apply(Long componentFileId) {

                    ArrayList<FileType3Feature> temps = new ArrayList<FileType3Feature>(){};
                    Exception exception = new JdbcUtils(jdbcTemplate).query(
                            String.format("select * from %s where component_id = ? and component_file_id = ?",job.getTask().getCurFileType3FeatureTable()),
                            FileType3Feature.class,
                            temps,
                            job.getTask().getComponentCacheInfo().componentId,componentFileId).error();

                    if (Objects.nonNull(exception)){
                        job.getTask().errorLog(String.format("******  Exception 缓存加载数据失败,table = %s; component_id= %s; component_file_id= %s",
                                job.getTask().getCurFileType3FeatureTable(),job.getTask().getComponentCacheInfo().componentId,componentFileId));
                        return null;
                    }

                    return temps.size()==0 ?
                        null:
                        new FileType3FeatureDTO(temps.get(0),
                            job.getTask().getCurFileType3FeatureTable(),
                            FileType3FeatureDTO.DATE_BASE_FLAG_SAVE_,
                            FileType3FeatureDTO.CACHE_FLAG_SAVE_);
                }
            });
    }


    private void getFileFeatureTable(Task task)throws Exception{
        String language = task.getLanguageTableKeyValue().toLowerCase();

        char[] chars = language.toCharArray();

        String fileFeatures = String.format("getSourcecode%s%sFiletype3featureName",
                String.valueOf(chars[0]).toUpperCase(),
                chars.length == 1 ? "" : String.valueOf(ArrayUtils.subarray(chars,1,chars.length)));

        Method method = ComponentCacheInfo.class.getMethod(fileFeatures);
        Object fileFeaturesName = method.invoke(task.getComponentCacheInfo());

        if(Objects.nonNull(fileFeaturesName)){
            task.setCurFileType3FeatureTable(fileFeaturesName.toString());
            task.getComponentCacheInfo().setNum(1);
            return;
        }


        //检查相关表数据数据量
        String tableName = "";
        for(int i = tableCount;i > 0 ;i--){
            tableName = String.format("t_sourcecode_%s_filetype3feature%d",language,i);
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
                tableName = String.format("t_sourcecode_%s_filetype3feature%d",language,i);
                break;
            }
        }

        task.setCurFileType3FeatureTable(tableName);
        task.getComponentCacheInfo().setNum(0);
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",task.getComponentCacheInfo().componentId)
                .update("sourcecode_"+language+"_filetype3feature_name = ?",tableName).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
    }



}
