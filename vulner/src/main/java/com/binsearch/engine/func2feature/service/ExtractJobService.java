package com.binsearch.engine.func2feature.service;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.engine.entity.db.FuncFeature;
import com.binsearch.engine.func2feature.WorkJob;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.ftp.FtpObjectPool;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import com.ubisec.UniSCAFossRadar.Preprocessor.Common.Constants.LanguageCode;
import com.ubisec.UniSCAFossRadar.Preprocessor.Feature.CodeFeature;
import com.ubisec.UniSCAFossRadar.Preprocessor.Feature.FuncsFeature;
import com.ubisec.UniSCAFossRadar.Preprocessor.Preprocessor;
import com.ubisec.UniSCAFossRadar.Preprocessor.PreprocessorContext;
import com.ubisec.UniSCAFossRadar.Preprocessor.SimplePreprocessor;
import com.ubisec.UniSCAFossRadar.Preprocessor.SimplePreprocessorContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

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
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Value("${func2feature.target.system.table.count}")
    Integer tableCount;

    @Value("${func2feature.target.system.data.count}")
    Integer dataCount;

    //是否开启ftp
    @Value("${func2feature.source.system.sftp.enable}")
    Boolean sFtpEnable;

    @Value("${func2feature.source.system.sftp.file.path}")
    String filePath;

    @Value("#{'${func2feature.source.system.sftp.connection}'.split(':')}")
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
            if(Strings.isEmpty(job.getCurFuncFeatureTable())) {
                synchronized (job.getComponentCacheInfo()) {
                    if(Strings.isEmpty(job.getCurFuncFeatureTable())){
                        //获取相关表
                        getFuncFeatureTable(job);
                    }
                }
            }

            ComponentFile componentFile = job.getComponentFile();
            String sourceFilePath = String.format("%s%s",sourceRootPath,componentFile.getFilePath());

            //目标文件处理
            String targetFile = filePath;
            targetFile = !targetFile.endsWith(File.separator)?targetFile + File.separator:targetFile;
            targetFile = targetFile + componentFile.getFilePath().replace("/","\\");

            //解析文件
            String text = "";
            LanguageCode languageCode = null;

            if(job.getLanguage().equalsIgnoreCase("JAVA")){
                languageCode = LanguageCode.JAVA;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getLanguage().equals("GOLANG")){
                languageCode = LanguageCode.GO;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getLanguage().equals("C")){
                languageCode = LanguageCode.C;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getLanguage().equals("C++")){
                languageCode = LanguageCode.CPP;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }else if(job.getLanguage().equals("PYTHON")){
                languageCode = LanguageCode.PYTHON;
                text = ftpObjectPool.getSourceFileText(sourceFilePath,targetFile);
            }

            if(Strings.isBlank(text)){
                job.errorLog(String.format("component.id= %s ,path= %s ,解析文件文本为空",componentFile.getComponentId(),sourceFilePath));
                return;
            }

            //解析源文件
            SimplePreprocessorContext context = new SimplePreprocessorContext();
            context.addPreprocessorContextType(SimplePreprocessorContext.Type.Type2Func);
            CodeFeature codeFeature = runningCodeTask(new CodeTask(new SimplePreprocessor(context),text,languageCode),job);

            if(Objects.isNull(codeFeature)){
                return;
            }

            FuncsFeature funcsFeature = (FuncsFeature) codeFeature.getFeature(CodeFeature.FeatureAttribute.FUNCS);
            if(CollectionUtils.isEmpty(funcsFeature.getFuncFeatures())){
                return;
            }

            List<FuncFeature> oldFuncFeatures = new ArrayList<FuncFeature>();
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .model(FuncFeature.class)
                    .table(job.getCurFuncFeatureTable())
                    .where("component_id = ? and component_file_id = ?",job.getComponentFile().componentId,job.getComponentFile().id)
                    .query(oldFuncFeatures)
                    .error();

            if(Objects.nonNull(exception)){
                job.errorLog(String.format("component.id = %s ,componentFile.id = %s , msg %s",job.getComponentFile().componentId,job.getComponentFile().id,exception.getMessage()));
                return;
            }

            List<FuncFeature> funcFeatures = new ArrayList<FuncFeature>(){};
            for(CodeFeature funcFeature : funcsFeature.getFuncFeatures()) {
                FuncFeature feature = new FuncFeature();
                feature.setComponentFileId(componentFile.getId());
                feature.setComponentId(componentFile.getComponentId());
                feature.setType0(funcFeature.getFeature(CodeFeature.FeatureAttribute.TYPE0).toString());
                feature.setType1(funcFeature.getFeature(CodeFeature.FeatureAttribute.TYPE1).toString());
                feature.setType2blind(funcFeature.getFeature(CodeFeature.FeatureAttribute.TYPE2Blind).toString());
                feature.setSourceTable(job.getComponentCacheInfo().getComponentFileName());
                feature.setStartLine(funcFeature.getStart());
                feature.setEndLine(funcFeature.getEnd());
                feature.setLineNumber(funcFeature.getLinenumber());
                feature.setTokenNumber((Integer)funcFeature.getFeature(CodeFeature.FeatureAttribute.TOKENNUMBER).getFeature());

                if(CollectionUtils.isEmpty(oldFuncFeatures)||
                        !isPresent(oldFuncFeatures,feature)){
                    funcFeatures.add(feature);
                }
            }


            if(!CollectionUtils.isEmpty(funcFeatures)){
                 exception = new JdbcUtils(jdbcTemplate, transactionTemplate)
                        .table(job.getCurFuncFeatureTable()).batchCreate(funcFeatures.toArray()).error();
                if (Objects.nonNull(exception)) {
                    throw exception;
                }
            }
        } catch (Exception e) {
            job.errorLog(String.format("****** Exception 初始化组件失败; component.id = %s; component_file_id = %s msg %s",
                    job.getComponentCacheInfo().componentId,job.getComponentFile().id,e.getMessage()));
        }finally {
            engineComponent.curWorkJobCountDecrement();
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
            job.errorLog(String.format("****** componentFile 分析错误; component.id = %s; component_file_id = %s; filepath = %s msg %s",
                    job.getComponentCacheInfo().componentId,job.getComponentFile().id,job.getComponentFile().getFilePath(),e.getMessage()));
        }
        return codeFeature;
    }



    public boolean isPresent(List<FuncFeature> funcsFeatures,FuncFeature feature){
        boolean flag = false;
        for(FuncFeature x:funcsFeatures){
           if(x.componentId.equals(feature.componentId) &&
                   x.componentFileId.equals(feature.componentFileId) &&
                   x.type1.equals(feature.type1) && x.type0.equals(feature.type0)){
               flag = true;
               break;
           }
       }
       return flag;
    }


    //获取组件文件存储表
    private void getFuncFeatureTable(WorkJob job)throws Exception{
        String language = job.getLanguageTableKeyValue().toLowerCase();

        char[] chars = language.toCharArray();

        String fileFeatures = String.format("getSourcecode%s%sFuncfeatureName",
                String.valueOf(chars[0]).toUpperCase(),
                chars.length == 1 ? "" : String.valueOf(ArrayUtils.subarray(chars,1,chars.length)));


        Method method = ComponentCacheInfo.class.getMethod(fileFeatures);
        Object fileFeaturesName = method.invoke(job.getComponentCacheInfo());

        if(Objects.nonNull(fileFeaturesName)){
            job.setCurFuncFeatureTable(fileFeaturesName.toString());
            job.getComponentCacheInfo().setNum(1);
            return;
        }

        //检查相关表数据数据量
        String tableName = "";
        for(int i = tableCount;i > 0 ;i--){
            tableName = String.format("t_sourcecode_%s_funcfeature%d",language,i);
            EntityCountNum num = new EntityCountNum();
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .table(tableName).count(num)
                    .error();

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
                tableName = String.format("t_sourcecode_%s_funcfeature%d",language,i);
                break;
            }
        }

        job.setCurFuncFeatureTable(tableName);
        job.getComponentCacheInfo().setNum(0);
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",job.getComponentCacheInfo().componentId)
                .update("sourcecode_"+language+"_funcfeature_name = ?",tableName).error();

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
