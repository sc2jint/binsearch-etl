package com.binsearch.engine.output.service;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.*;
import com.binsearch.engine.output.WorkJob;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.orm.JdbcUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import javax.persistence.Column;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.binsearch.engine.output.EngineConfiguration.OUTPUTSQL_ENGINE;
import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TEMPLATE;
import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TRANSACTION_TEMPLATE;

public class ExtractJobService {

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
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;


    //结果表集
    public ConcurrentHashMap<String, List<File>> resultFileMap = new ConcurrentHashMap<String,List<File>>();


     @Value("${outputsql.script.save.path}")
     String savePath;


     //文件切割数
    @Value("${outputsql.script.file.result.pageCutNum}")
    Integer pageCutNum;

    //导出组件的数据类型
    @Value("${outputsql.script.fullComponent.data.model.type}")
    String dataModelType;

    //导出组件的开发语言
    @Value("#{'${outputsql.script.fullComponent.data.component.languages}'.split(',')}")
    List<String> dataComponentlanguages;

    AtomicInteger count = new AtomicInteger(0);



    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void featureExtract() {
        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS);

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
        }finally {
            pipeLineComponent.threadNumIncrement();
        }
    }


    private void featureExtract(WorkJob job) {
        //组件语言过滤
        if(!dataComponentlanguages.contains(job.getCacheInfo().mainLanguage.toUpperCase(Locale.getDefault()))){
            return;
        }

        try{
            while (true) {
                updateWorkJob(job);

                if(Strings.isBlank(job.getCurrentTableNameValue())){
                    break;
                }

                //
                if(resultFileMap.get(job.getCurrentTableNameValue()) == null){
                    synchronized (resultFileMap){
                        if(resultFileMap.get(job.getCurrentTableNameValue()) == null){
                            String path = savePath;
                            if(!path.endsWith(File.separator)){
                                path += File.separator;
                            }

                            ArrayList<File> temp = new ArrayList<>();
                            for(int i = 1;i <= pageCutNum;i++) {
                                String sql = path + job.getCurrentTableNameValue()+File.separator+job.getCurrentTableNameValue() +String.format("_%d.sql",i);
                                File file = new File(sql);
                                if(!file.getParentFile().exists()){
                                    file.getParentFile().mkdirs();
                                }
                                temp.add(file);
                            }
                            resultFileMap.put(job.getCurrentTableNameValue(),temp);
                        }
                    }
                }


                if(dataModelType.contains("componentFile") && job.getCurrentTableNameValue().contains("_component_file")){
                    //创建导出临时表
                    running(job, ComponentFile.class);
                }

                if(dataModelType.contains("fileFeature") && job.getCurrentTableNameValue().contains("_filefeature")){
                    //创建导出临时表
                    running(job, FileFeature.class);
                }

                if(dataModelType.contains("funcFeature") && job.getCurrentTableNameValue().contains("_funcfeature")){
                    //创建导出临时表
                    running(job, FuncFeature.class);
                }
            }
        }catch (Exception e){
            try {
                saveOutPutErrorLog("", String.format("featureExtract异常,%s", e.getMessage()), "");
            }catch (Exception s){}
        }finally {
            engineComponent.curWorkJobCountDecrement();
        }
    }

    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void fileSqlWrite(String tableName,List components,Class clazz){
        count.getAndIncrement();
        try{
            if(!CollectionUtils.isEmpty(components)) {
                StringBuilder builder = new StringBuilder();
                for (Object item : components) {
                    if(clazz.equals(ComponentFile.class)) {
                        ComponentFile componentFile = (ComponentFile) item;
                        String temp = String.format("insert into %s(`id`,`component_id`,`source_file_path`,`file_path`,`language`," +
                                        "`component_version_id`,`file_hash_value`)values(%d,'%s','%s','%s','%s','%s','%s');\n",
                                tableName,
                                Objects.isNull(componentFile.getId()) ? 0 : componentFile.getId(),
                                Objects.isNull(componentFile.getComponentId()) ? "" : componentFile.getComponentId(),
                                Objects.isNull(componentFile.getSourceFilePath()) ? "" : componentFile.getSourceFilePath().replace("\\","/"),
                                Objects.isNull(componentFile.getFilePath()) ? "" : componentFile.getFilePath(),
                                Objects.isNull(componentFile.getLanguage()) ? "" : componentFile.getLanguage(),
                                Objects.isNull(componentFile.getComponentVersionId()) ? "" : componentFile.getComponentVersionId(),
                                Objects.isNull(componentFile.getFileHashValue()) ? "" : componentFile.getFileHashValue());
                        builder.append(temp);
                    }else if(clazz.equals(FileFeature.class)){
                        FileFeature fileFeature = (FileFeature) item;
                        String temp = String.format("insert into %s(`id`,`component_file_id`,`component_id`," +
                                        "`line_number`,`token_number`,`type0`,`type1`,`type2blind`,`source_table`)values(" +
                                        "%d,%d,'%s',%d,%d,'%s','%s','%s','%s');\n",
                                tableName,
                                Objects.isNull(fileFeature.getId())?0:fileFeature.getId(),
                                Objects.isNull(fileFeature.getComponentFileId())?0:fileFeature.getComponentFileId(),
                                Objects.isNull(fileFeature.getComponentId())?"":fileFeature.getComponentId(),
                                Objects.isNull(fileFeature.getLineNumber())?0:fileFeature.getLineNumber(),
                                Objects.isNull(fileFeature.getTokenNumber())?0:fileFeature.getTokenNumber(),
                                Objects.isNull(fileFeature.getType0())?"":fileFeature.getType0(),
                                Objects.isNull(fileFeature.getType1())?"":fileFeature.getType1(),
                                Objects.isNull(fileFeature.getType2blind())?"":fileFeature.getType2blind(),
                                Objects.isNull(fileFeature.getSourceTable())?"":fileFeature.getSourceTable());
                        builder.append(temp);
                    }else{
                        FuncFeature funcFeature =(FuncFeature)item;
                        String temp = String.format("insert into %s(`id`,`component_file_id`,`component_id`,`start_line`,`end_line`," +
                                        "`line_number`,`token_number`,`type0`,`type1`,`type2blind`,`source_table`)values(" +
                                        "%d,%d,'%s',%d,%d,%d,%d,'%s','%s','%s','%s');\n",
                                tableName,
                                Objects.isNull(funcFeature.getId())?0:funcFeature.getId(),
                                Objects.isNull(funcFeature.getComponentFileId())?0:funcFeature.getComponentFileId(),
                                Objects.isNull(funcFeature.getComponentId())?"":funcFeature.getComponentId(),
                                Objects.isNull(funcFeature.getStartLine())?0:funcFeature.getStartLine(),
                                Objects.isNull(funcFeature.getEndLine())?0:funcFeature.getEndLine(),
                                Objects.isNull(funcFeature.getLineNumber())?0:funcFeature.getLineNumber(),
                                Objects.isNull(funcFeature.getTokenNumber())?0:funcFeature.getTokenNumber(),
                                Objects.isNull(funcFeature.getType0())?"":funcFeature.getType0(),
                                Objects.isNull(funcFeature.getType1())?"":funcFeature.getType1(),
                                Objects.isNull(funcFeature.getType2blind())?"":funcFeature.getType2blind(),
                                Objects.isNull(funcFeature.getSourceTable())?"":funcFeature.getSourceTable());
                        builder.append(temp);
                    }
                }

                List<File> files = resultFileMap.get(tableName);
                File file = files.get(RandomUtils.nextInt(0, pageCutNum));
                synchronized (file) {
                    FileUtils.write(file, builder.toString(), Charset.defaultCharset(), true);
                }
            }
        }catch (Exception e) {
            try {
                saveOutPutErrorLog("", String.format("%s文件写出错,%s",tableName,e.getMessage()), "");
            }catch (Exception w){}
        }finally {
            if(count.get()>0){
                count.getAndDecrement();
            }
        }
    }

    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void querySQLData(WorkJob job,Integer s,Integer e,Class clazz){
        try {
            List components = new ArrayList<>();
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .query(
                            String.format("select * from %s where id >= ? and id < ? and component_id = ?",job.getCurrentTableNameValue()),
                            clazz,components,s, e,job.getCacheInfo().getComponentId())
                    .error();

            if (Objects.nonNull(exception)) {
                throw exception;
            }

            fileSqlWrite(job.getCurrentTableNameValue(),components,clazz);
        }catch (Exception ex){
            try {
                saveOutPutErrorLog("","批量导出出错:"+ex.getMessage(),"");
            }catch (Exception ex1) {}
        }
    }

    private void running(WorkJob job,Class clazz){
        List<Statistical> statisticals = new ArrayList<>();
        new JdbcUtils(jdbcTemplate)
                .query(String.format("select max(id) as max_num,min(id) as min_num,count(id) as count_num from %s where component_id = ?",job.getCurrentTableNameValue()),
                        Statistical.class,statisticals,job.getCacheInfo().getComponentId()).error();

        if(CollectionUtils.isEmpty(statisticals)){
            try {
                saveOutPutErrorLog("", String.format("component.id = %s,table = %s 中没有数据",job.getCacheInfo().getComponentId(),job.getCurrentTableNameValue()), "");
            }catch (Exception e){}
            return;
        }

        if(statisticals.get(0).countNum == 0){
            try {
                saveOutPutErrorLog("", String.format("component.id = %s,table = %s 中没有数据",job.getCacheInfo().getComponentId(),job.getCurrentTableNameValue()), "");
            }catch (Exception e){}
            return;
        }

        int pageSize_ = 300;
        int pageSize = pageSize_;//每页数据量
        int curPage = 1;//当前页数
        int pageNum = 1;//页数

        if(statisticals.get(0).countNum > pageSize){
            pageNum = ((statisticals.get(0).countNum) - 1) / pageSize + 1;
            pageSize = (statisticals.get(0).getMaxNum() - statisticals.get(0).getMinNum()) / pageNum;
        }

        while(true){
            if(curPage > pageNum){
                break;
            }

            if(count.get()>10000){
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {}
                continue;
            }

            int s = statisticals.get(0).getMinNum();
            int e = statisticals.get(0).getMaxNum();

            if(statisticals.get(0).countNum > pageSize_){
                s = statisticals.get(0).getMinNum()+((curPage - 1) * pageSize);
                e = statisticals.get(0).getMinNum()+(curPage*pageSize);
            }

            curPage++;
            querySQLData(job,s,e,clazz);
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

    private void updateWorkJob(WorkJob job){
        try {
            job.setCurrentTableNameValue(null);

            Field[] fields = job.getCacheInfo().getClass().getFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    if(column.name().endsWith("_name")){
                        if(Strings.isBlank(job.getCurrentTableName())||
                                !job.getCurrentTableName().contains(column.name())){
                            char[] chars = field.getName().toCharArray();

                            String methodName = String.format("get%s%s",
                                    String.valueOf(chars[0]).toUpperCase(),
                                    String.valueOf(ArrayUtils.subarray(chars, 1, chars.length)));

                            Method method = ComponentCacheInfo.class.getMethod(methodName);
                            Object value = method.invoke(job.getCacheInfo());
                            if(Objects.isNull(value)){
                                continue;
                            }

                            job.setCurrentTableNameValue(value.toString());
                            if(Strings.isBlank(job.getCurrentTableName())){
                                job.setCurrentTableName(column.name()+";");
                            }else{
                                job.setCurrentTableName(job.getCurrentTableName()+column.name()+";");
                            }


                            if(Strings.isNotBlank(job.getCurrentTableNameValue())){
                                break;
                            }
                        }else{
                            job.setCurrentTableNameValue(null);
                        }
                    }
                }
            }
        }catch (Exception e){
            job.setCurrentTableNameValue(null);
            try {
                saveOutPutErrorLog("",String.format("切换表信息出错,%s",e.getMessage()), "");
            }catch (Exception r){}
        }
    }
}
