package com.binsearch.engine.component2file.service;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.Constant;
import com.binsearch.engine.component2file.Engine;
import com.binsearch.engine.component2file.EngineConfiguration;
import com.binsearch.engine.component2file.WorkJob;
import com.binsearch.engine.component2file.ComponentFileDTO;
import com.binsearch.engine.entity.db.*;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.ftp.FtpObjectPool;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.DigestUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * 解压提取文件
 * */
public class ExtractJobService {

    @Value("${base.system.targetFileRootPath}")
    private String targetFileRootPath;

    @Value("${base.system.targetFileDir}")
    private String targetFileDir;

    @Value("${component2file.target.system.data.count}")
    Integer dataCount;

    @Value("${component2file.target.system.table.count}")
    Integer tableCount;

    @Value("${base.cache.maximum}")
    long cacheMaximum;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_BASE_THREAD_POOL)
    Executor simpleAsync;

    @Autowired
    @Qualifier(EngineConfiguration.COMPONENT2FILE_ANALYSIS_JOB_SERVICE)
    AnalysisJobService analysisJobService;

    //是否开启ftp
    @Value("${component2file.source.system.sftp.enable}")
    Boolean sFtpEnable;

    @Value("#{'${component2file.source.system.sftp.connection}'.split(':')}")
    List<String> sFtpInfos;

    @Value("${component2file.source.system.sftp.file.path}")
    String sFtpFilePath;


    FtpObjectPool ftpObjectPool;


    /**
     * 重复检数据查获取异步任务
     */
    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void fileExtract(){
        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS);

        //远程访问池初始化
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
                        } catch (Exception e) {}
                        continue;
                    }
                }
                fileExtract((WorkJob) job);
            }
        }catch (Exception e){}
        finally {
            pipeLineComponent.threadNumIncrement();
        }
    }

    /**
     * 组件关系数据提取
     * */
    public void fileExtract(WorkJob job){
        try{
            //获取存储的表
            if(Objects.isNull(job.getTask().getLoadingCache())) {
                synchronized (job.getTask()) {
                    if (Objects.isNull(job.getTask().getLoadingCache())) {
                        job.getTask().log("///////////////// ExtractJobService /////////////////");

                        //组件目标路径
                        String componentDirPath = String.format("%s/%s/%s", targetFileRootPath, targetFileDir, job.getTaskId());
                        if (!ftpObjectPool.createDir(componentDirPath)) {
                            job.errorLog(String.format("componentDirPath = %s,文件夹创建失败", componentDirPath));
                            return;
                        }
                        job.getTask().log(String.format("******  componentDirPath = %s;", componentDirPath));

                        getComponentFileTable(job.getTask().getComponentCacheInfo());

                        //实例化缓存
                        job.getTask().initLoadingCache(cacheMaximum, simpleAsync, new RemovalListener<String, ComponentFileDTO>() {
                            @Override
                            public void onRemoval(@Nullable String key, @Nullable ComponentFileDTO value, RemovalCause cause) {
                                try {
                                    if (Objects.isNull(value)) return;

                                    while (true) {
                                        if (value.getCacheFlag() != ComponentFileDTO.CACHE_FLAG_SAVE_) {
                                            try {
                                                TimeUnit.MILLISECONDS.sleep(100L);
                                            } catch (Exception e) {
                                            }
                                            continue;
                                        }

                                        synchronized (value) {
                                            if(value.getCacheFlag() != ComponentFileDTO.CACHE_FLAG_SAVE_){
                                                continue;
                                            }
                                            if (value.getSystemFileFlag() == ComponentFileDTO.SYSTEM_FILE_FLAG_UN_SAVE_ &&
                                                    Objects.nonNull(value.getFileBytes()) && value.getFileBytes().length > 0) {

                                                //写文件
                                                String stringBuilder = String.format("%s/%s", targetFileRootPath, value.getComponentFile().getFilePath());
                                                if (!ftpObjectPool.isFileExist(stringBuilder.replace("//","/"))) {
                                                    ftpObjectPool.upload(value.getFileBytes(),stringBuilder);
                                                }

                                                value.setSystemFileFlag(ComponentFileDTO.SYSTEM_FILE_FLAG_SAVE_);
                                                value.setFileBytes(null);
                                            }

                                            if (value.getDataBaseFlag() == ComponentFileDTO.DATE_BASE_FLAG_UN_SAVE_) {
                                                Exception exception = null;

                                                if (Objects.nonNull(value.getComponentFile().getId()) && value.getComponentFile().getId() > 0) {

                                                    String taskId = job.getSourceType().equals(Engine.SOURCE_TYPE_GIT)?
                                                            ((ViewTasks)job.getTask().getViewTasks()).taskId:((MavenTasks)job.getTask().getViewTasks()).compoId;
                                                    exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                                                            .table(job.getTask().getComponentCacheInfo().getComponentFileName())
                                                            .where("component_id = ? and file_hash_value = ?", taskId, value.getComponentFile().getFileHashValue())
                                                            .update("source_file_path = ?,component_version_id = ?", value.getComponentFile().getSourceFilePath(), value.getComponentFile().getComponentVersionId())
                                                            .error();
                                                } else {
                                                    exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                                                            .table(job.getTask().getComponentCacheInfo().getComponentFileName()).create(value.getComponentFile()).error();
                                                }

                                                if (Objects.nonNull(exception)) {
                                                    throw exception;
                                                }
                                                value.setDataBaseFlag(ComponentFileDTO.DATE_BASE_FLAG_SAVE_);
                                            }
                                        }
                                        break;
                                    }
                                } catch (Exception e) {
                                    job.errorLog(String.format("******  Exception 缓存清除事件失败,component.id = %s; filePath = %s; fileHashValue = %s; %s",
                                            value.getComponentFile().getComponentId(), value.getComponentFile().getFilePath(), value.getComponentFile().getFileHashValue(), e.getMessage()));
                                }
                            }
                        });

                        //初始化组件缓存
                        if (job.getTask().getComponentCacheInfo().getNum() > 0) {
                            List<ComponentFile> componentFiles = getComponentFiles(
                                    job.getTaskId(),
                                    job.getTask().getComponentCacheInfo().getComponentFileName());

                            //初始化缓存
                            for (ComponentFile componentFile : componentFiles) {
                                job.getTask().getLoadingCache().put(
                                    componentFile.getFileHashValue(),
                                    new ComponentFileDTO(
                                        componentFile,
                                        job.getTask().getComponentCacheInfo().getComponentFileName(),
                                        ComponentFileDTO.DATE_BASE_FLAG_SAVE_,
                                        ComponentFileDTO.SYSTEM_FILE_FLAG_SAVE_,
                                        ComponentFileDTO.CACHE_FLAG_SAVE_));
                            }
                            job.getTask().log(String.format("******  initCacheSize = %s", componentFiles.size()));
                        }
                    }
                }
            }
            //分析文件
            job.getTask().log(String.format("******  component.id = %s; versionName = %s;", job.getTaskId(),job.getVersionName()));
            extractZipFile(job);
        }catch (Exception e){
            job.errorLog(String.format("****** Exception 初始化组件失败; component.id = %s; version.size = %s msg %s",job.getTaskId(),job.getTask().getVersionNum(),e.getMessage()));
        }finally {
            //同步缓存
            try {
                analysisJobService.synCache(job);
            }catch (Exception e) {}

            //更新组件版本信息
            updateComponentVersion(job);

            //检查是否所有版本分析完
            job.getTask().versionDecrement();
            if(job.getTask().getVersionNum() <= 0) {
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.ANALYSIS_JOBS).addPipeLineJob(job);
            }else{
                engineComponent.curWorkJobCountDecrement();
            }
        }
    }


        //更新组件完成版本
    public void updateComponentVersion(WorkJob job){
        try{
             synchronized (job.getTask().getComponentCacheInfo()) {
                String versionName = job.getVersionName();

                //处理后缀名
                if(versionName.contains(".zip")){
                    versionName = versionName.replace(".zip",";");
                }else if(versionName.contains(".jar")){
                    versionName = versionName.replace(".jar",";");
                }else{
                    versionName = versionName+";";
                }

                //获取缓存信息
                ComponentCacheInfo componentCacheInfo = job.getTask().getComponentCacheInfo();

                if((Strings.isBlank(componentCacheInfo.getComponentVersion())||
                        !componentCacheInfo.getComponentVersion().contains(versionName))){

                     String version = String.format("%s%s;",
                             Strings.isBlank(componentCacheInfo.getComponentVersion())?
                                     "":componentCacheInfo.getComponentVersion(),versionName);

                     componentCacheInfo.setComponentVersion(version);

                     Exception exception = new JdbcUtils(etlJdbcTemplate)
                            .model(ComponentCacheInfo.class)
                            .where("component_id = ?",job.getTaskId())
                            .update("component_version = ?",componentCacheInfo.getComponentVersion())
                            .error();

                    if (Objects.nonNull(exception)) {
                        throw exception;
                    }
                }
            }
        }catch (Exception e){
            job.errorLog(String.format("更新组件版本出错,%s",e.getMessage()));
        }
    }

    //获取组件文件数据
    public List<ComponentFile> getComponentFiles(String componentId,String componentFileTable)throws Exception{
        List<ComponentFile> componentFile = new ArrayList<ComponentFile>(){};
        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(ComponentFile.class)
                .table(componentFileTable)
                .where("component_id = ?",componentId)
                .limit(cacheMaximum)
                .query(componentFile)
                .error();
        if(Objects.nonNull(exception)){
            throw exception;
        }
        return componentFile;
    }



    public void extractZipFile(WorkJob job) {
        String componentVersionId,sourceFilePath;

        //获取组件versionid,sourceFilePath
        if(job.getSourceType().equals(Engine.SOURCE_TYPE_GIT)){
            componentVersionId = job.getVersionName();
            componentVersionId = componentVersionId.substring(0, componentVersionId.lastIndexOf("."));

            //组件源码路径
            sourceFilePath = String.format("%s/%s/%s", job.getSourceFilePath(), job.getTaskId(), job.getVersionName());
        }else{
            componentVersionId = job.getVersionName();
            sourceFilePath = String.format("%s/%s",job.getSourceFilePath(),((MavenDetailTasks)job.getDetailTask()).getSourceCodeAddr());
        }
        sourceFilePath = sourceFilePath.replace("//","/");

        //判断目标的后缀
        String targetFile = sFtpFilePath;
        targetFile = !targetFile.endsWith(File.separator) ? targetFile + File.separator : targetFile;
        targetFile = targetFile + sourceFilePath.replace("/","\\");
        targetFile = targetFile.replace("\\\\","\\");


        //获取文件路径
        sourceFilePath = ftpObjectPool.getSourceFilePath(sourceFilePath,targetFile);

        if(Strings.isBlank(sourceFilePath)){
            job.errorLog(String.format("component.id = %s, sourceFilePath = %s,源文件不存在",job.getTaskId(),sourceFilePath));
            return;
        }

        File sourceFile = null;
        ZipFile zip = null;
        int itemCount = 0;
        try{
             sourceFile = new File(sourceFilePath);

            //检查源文件是否存在
            if (!sourceFile.exists() || !sourceFile.isFile()) {
                job.errorLog(String.format("component.id = %s, sourceFilePath = %s,源文件不存在",job.getTaskId(), sourceFile.getAbsolutePath()));
                return;
            }

            job.getTask().log(String.format("******  sourceFilePath = %s; file.size = %s;",sourceFile.getAbsolutePath(),getPrintSize(sourceFile.length())));

            zip = new ZipFile(sourceFile);
            for (Enumeration entries = zip.entries(); entries.hasMoreElements(); ) {
                synComponentFileCache(zip,entries,job,componentVersionId);
                itemCount++;
            }
            job.getTask().log(String.format("******  versionName = %s; itemCount = %s;",job.getVersionName(),itemCount));
        } catch (Exception e) {
            job.errorLog(String.format("******  Exception 打开解压文件出错;以解压总数 %d;%s;%s",itemCount,sourceFile.getAbsolutePath(),e.getMessage()));
        }finally {
            if(Objects.nonNull(zip)){
                try {
                    zip.close();
                } catch (IOException e) {}
            }
        }
    }


    public void synComponentFileCache(ZipFile zip,Enumeration entries,WorkJob job, String componentVersionId){
        ZipEntry entry = (ZipEntry) entries.nextElement();
        String curEntryName = entry.getName();
        // 判断文件夹
        if (Strings.isEmpty(curEntryName) || curEntryName.endsWith("/")) {
            return;
        }

        for(Constant.LanguageType languageType:Constant.LanguageType.values()){
            for (String expandName:languageType.getExpandNames()){
                if(!curEntryName.endsWith("."+expandName)){
                    continue;
                }

                InputStream in = null;
                byte[] fileBytes = null;
                try{
                    in = zip.getInputStream(entry);
                    fileBytes = IOUtils.toByteArray(in);
                } catch (Exception ex) {
                    job.errorLog(String.format("******  Exception 分析文件读取字节码出错;%s;%s;",curEntryName,ex.getMessage()));
                } finally {
                    if(Objects.nonNull(in)){
                        try {
                            in.close();
                        } catch (Exception e) {}
                    }
                }

                if(Objects.nonNull(fileBytes) && fileBytes.length > 0){
                    String fileHashValue = DigestUtils.md5DigestAsHex(fileBytes);
                    ComponentFile componentFile = new ComponentFile();
                    componentFile.setFileHashValue(fileHashValue);
                    componentFile.setSourceFilePath(String.format("%s@%s;",componentVersionId,curEntryName.contains("/")?curEntryName.replaceAll("/","\\\\"):curEntryName));
                    componentFile.setComponentId(job.getTaskId());
                    componentFile.setComponentVersionId(componentVersionId+";");
                    componentFile.setLanguage(languageType.getLanguageName());
                    componentFile.setFilePath(String.format("/%s/%s/%s",targetFileDir,job.getTaskId(),fileHashValue));

                    synComponentFileCache(job,componentFile,fileBytes);
                }
            }
        }
    }

    //从数据库中加组件缓存信息到缓存
    private ComponentFileDTO getCacheComponentFileDTO(WorkJob job,ComponentFile componentFile){
        return job.getTask().getLoadingCache()
            .get(componentFile.getFileHashValue(), new Function<String, ComponentFileDTO>() {
                @Override
                public ComponentFileDTO apply(String fileHashValue) {

                    ArrayList<ComponentFile> temp = new ArrayList<ComponentFile>(){};
                    Exception exception = new JdbcUtils(jdbcTemplate).query(
                            String.format("select * from %s where component_id = ? and file_hash_value = ?",job.getTask().getComponentCacheInfo().getComponentFileName()),
                            ComponentFile.class,
                            temp,
                            job.getTaskId(),
                            fileHashValue).error();

                    if (Objects.nonNull(exception)){
                        job.errorLog(String.format("******  Exception 缓存加载数据失败,table = %s; component_id= %s; file_hash_value= %s",
                                job.getTask().getComponentCacheInfo().getComponentFileName(),job.getTaskId(),fileHashValue));
                        return null;
                    }

                    return temp.size()==0 ?
                            null:
                            new ComponentFileDTO(temp.get(0),
                                    job.getTask().getComponentCacheInfo().getComponentFileName(),
                                    ComponentFileDTO.DATE_BASE_FLAG_SAVE_,
                                    ComponentFileDTO.SYSTEM_FILE_FLAG_SAVE_,
                                    ComponentFileDTO.CACHE_FLAG_SAVE_);
                }
            });
    }

    //同步缓存
    public void synComponentFileCache(WorkJob job,ComponentFile componentFile,byte[] fileBytes) {
        Integer action = 0;

        ComponentFileDTO dto = getCacheComponentFileDTO(job, componentFile);
        if (Objects.isNull(dto)) {
            action = ComponentFileDTO.CACHE_ACTION_CREAT_;
        } else {
            action = ComponentFileDTO.CACHE_ACTION_UPDATE_;
        }

        if (action == ComponentFileDTO.CACHE_ACTION_CREAT_) {
            synchronized (job.getTask()) {
                dto = getCacheComponentFileDTO(job, componentFile);
                if (Objects.nonNull(dto)) {
                    action = ComponentFileDTO.CACHE_ACTION_UPDATE_;
                } else {
                    dto = new ComponentFileDTO();
                    dto.setCacheFlag(ComponentFileDTO.CACHE_FLAG_UN_SAVE_);
                    dto.setCurComponentFileTable(job.getTask().getComponentCacheInfo().getComponentFileName());
                    dto.setDataBaseFlag(ComponentFileDTO.DATE_BASE_FLAG_UN_SAVE_);
                    dto.setSystemFileFlag(ComponentFileDTO.SYSTEM_FILE_FLAG_UN_SAVE_);
                    dto.setComponentFile(componentFile);
                    dto.setFileBytes(fileBytes);
                    job.getTask().getLoadingCache().put(componentFile.getFileHashValue(), dto);
                    dto.setCacheFlag(ComponentFileDTO.CACHE_FLAG_SAVE_);
                }
            }
        }

        if (action == ComponentFileDTO.CACHE_ACTION_UPDATE_) {
            while (true) {
                dto = getCacheComponentFileDTO(job, componentFile);

                if(dto.getCacheFlag() != ComponentFileDTO.CACHE_FLAG_SAVE_){
                    try {
                        TimeUnit.MILLISECONDS.sleep(50);
                    } catch (Exception e) {}
                    continue;
                }

                synchronized (dto) {
                    if(dto.getCacheFlag() != ComponentFileDTO.CACHE_FLAG_SAVE_){
                        continue;
                    }
                    dto.setCacheFlag(ComponentFileDTO.CACHE_FLAG_UN_SAVE_);
                    if (!dto.getComponentFile().getSourceFilePath().contains(componentFile.getSourceFilePath())) {
                        dto.getComponentFile().setSourceFilePath(String.format("%s%s", dto.getComponentFile().getSourceFilePath(), componentFile.getSourceFilePath()));
                        dto.getComponentFile().setComponentVersionId(String.format("%s%s", dto.getComponentFile().getComponentVersionId(), componentFile.getComponentVersionId()));
                        dto.setDataBaseFlag(ComponentFileDTO.DATE_BASE_FLAG_UN_SAVE_);
                        job.getTask().getLoadingCache().put(componentFile.getFileHashValue(), dto);
                    }

                    if(!ftpObjectPool.isFileExist(String.format("%s/%s", targetFileRootPath, dto.getComponentFile().getFilePath()).replace("//","/"))){
                        dto.setSystemFileFlag(ComponentFileDTO.SYSTEM_FILE_FLAG_UN_SAVE_);
                        dto.setFileBytes(fileBytes);
                        job.getTask().getLoadingCache().put(componentFile.getFileHashValue(), dto);
                    }


                    dto.setCacheFlag(ComponentFileDTO.CACHE_FLAG_SAVE_);
                }

                break;
            }
        }
    }


    /**
     * @Description:计算文件大小
     * @Param: [size]
     * @return: java.lang.String
     * @Date: 2020/5/19
     */
    public static String getPrintSize(long size) {
        // 如果字节数少于1024，则直接以B为单位，否则先除于1024，后3位因太少无意义
        if (size < 1024) {
            return String.valueOf(size) + "BB";
        } else {
            size = size / 1024;
        }
        // 如果原字节数除于1024之后，少于1024，则可以直接以KB作为单位
        // 因为还没有到达要使用另一个单位的时候
        // 接下去以此类推
        if (size < 1024) {
            return String.valueOf(size) + "KB";
        } else {
            size = size / 1024;
        }
        if (size < 1024) {
            // 因为如果以MB为单位的话，要保留最后1位小数，
            // 因此，把此数乘以100之后再取余
            size = size * 100;
            return String.valueOf((size / 100)) + "." + String.valueOf((size % 100)) + "MB";
        } else {
            // 否则如果要以GB为单位的，先除于1024再作同样的处理
            size = size * 100 / 1024;
            return String.valueOf((size / 100)) + "." + String.valueOf((size % 100)) + "GB";
        }
    }




    //获取组件文件存储表
    private void getComponentFileTable(ComponentCacheInfo cacheInfo)throws Exception{
        if(Strings.isNotBlank(cacheInfo.getComponentFileName())){
            //更新componentCache状态
            Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",cacheInfo.getComponentId())
                .update("component2file_flag = ?,file2feature_flag = ?,func2feature_flag =?,file2type3feature_flag = ?",
                        ComponentCacheInfo.RUNNING_FLAGS_UN_END,
                        ComponentCacheInfo.RUNNING_FLAGS_UN_END,
                        ComponentCacheInfo.RUNNING_FLAGS_UN_END,
                        ComponentCacheInfo.RUNNING_FLAGS_UN_END)
                .error();

            if (Objects.nonNull(exception)) {
                throw exception;
            }

            cacheInfo.setNum(1);
            return;
        }

        //检查相关表数据数据量
        String tableName = "";
        for(int i = tableCount;i > 0 ;i--){
            tableName = String.format("t_component_file%d",i);
            EntityCountNum num = new EntityCountNum();
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .table(tableName)
                    .count(num).error();

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
                tableName = String.format("t_component_file%d",i);
                break;
            }
        }


        cacheInfo.setComponentFileName(tableName);
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",cacheInfo.getComponentId())
                .update("component_file_name = ?",cacheInfo.getComponentFileName()).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
    }
}




