package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.Constant;
import com.binsearch.engine.component2file.ComponentFileDTO;
import com.binsearch.engine.entity.db.*;
import com.binsearch.engine.entity.el.ComponentFileES;
import com.binsearch.engine.entity.el.FileFeatureES;
import com.binsearch.engine.entity.el.FuncFeatureES;
import com.binsearch.engine.entity.el.index.ComponentFileIndexES;
import com.binsearch.engine.entity.el.index.FileFeatureIndexES;
import com.binsearch.engine.entity.el.index.FuncFeatureIndexES;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.orm.JdbcUtils;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.binsearch.engine.elasticsearch.EngineConfiguration.ELASTICSEARCH_ENGINE;
import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TEMPLATE;


public class ExtractJobService {
    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(ExtractJobService.class);

    @Autowired
    @Qualifier(EngineConfiguration.ELASTICSEARCH_CLIENT)
    ElasticsearchRestTemplate elTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    FileFeatureDao fileFeatureDao;

    @Value("${base.cache.maximum}")
    long cacheMaximum;

    private AtomicInteger count = new AtomicInteger(0);


    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void dataExtract(){
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
                        } catch (Exception e) {}
                        continue;
                    }
                }
                dataExtract((WorkJob) job);
            }
        }catch (Exception e){}
        finally {
            pipeLineComponent.threadNumIncrement();
        }
    }


    private String getFieldValue(Constant.LanguageType languageType,Constant.ModelType modelType,WorkJob job){
        try {
            Field field = null;
            char[] chars = languageType.getAlias().toCharArray();

            String attrName = String.format("%s%s",
                    String.valueOf(chars[0]).toUpperCase(),
                    chars.length == 1 ? "" : String.valueOf(ArrayUtils.subarray(chars, 1, chars.length)));

            attrName = String.format(modelType.getComponentCacheAttrName(), attrName);

            //判断字段是否存在
            field = modelType.getEntityClass().getDeclaredField(attrName);
            if (Objects.isNull(field)) {
                return "";
            }

            //打开权限
            field.setAccessible(true);

            //获取值
            return (String) field.get(job.getTask().getComponentCacheInfo());
        }catch (Exception e){

        }
        return "";
    }

    public void saveESErrorLog(String id,String log,String target){
        EngineRunningError error = new EngineRunningError();
        error.setComponentId(id);
        error.setErrorDate(new Timestamp(System.currentTimeMillis()));
        error.setRunningErrorInfo(log);
        error.setTargetTable(target);
        error.setEngineType(ELASTICSEARCH_ENGINE);

        new JdbcUtils(etlJdbcTemplate).create(error);
    }

    private void loadCacheData(WorkJob job){
        if(job.getCurrentModelType() == Constant.ModelType.MODEL_COMPONENT_FILE){

        }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FILE_FEATURE){
            List<FileFeatureES> result = fileFeatureDao.findByComponentId(job.getTask().getComponentCacheInfo().getComponentId());

            if(!CollectionUtils.isEmpty(result)){
                result.stream().forEach(item->{
                    job.getTask().getLoadingCache().put(
                            item.getId(),
                            new ESCacheDTO(item,ESCacheDTO.DATE_BASE_FLAG_SAVE_));
                });
            }
        }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FUNC_FEATURE){

        }

    }

    private void initLoadingCache(WorkJob job){
        job.getTask().initLoadingCache(cacheMaximum,new RemovalListener<String, ESCacheDTO>() {
            @Override
            public void onRemoval(@Nullable String key, @Nullable ESCacheDTO value, RemovalCause cause) {

                if (Objects.isNull(value)) return;

                if (value.getCacheFlag() == ESCacheDTO.DATE_BASE_FLAG_UN_SAVE_){
                    synchronized (value){
                        if(value.getCacheFlag() == ESCacheDTO.DATE_BASE_FLAG_UN_SAVE_){
                            try {
                                if (value.getCacheData() instanceof ComponentFileES){

                                }
                                if (value.getCacheData() instanceof FileFeatureES){
                                    FileFeatureES es = (FileFeatureES)value.getCacheData();
                                    fileFeatureDao.save(es);
                                }
                                if (value.getCacheData() instanceof FuncFeatureES){

                                }

                            } catch (Exception e) {
                                saveESErrorLog("","数据退出缓存失败,"+e.getMessage(),"");
                            }finally {
                                value.setCacheFlag(ESCacheDTO.DATE_BASE_FLAG_SAVE_);
                            }
                        }
                    }
                }
            }
        });
    }



    private void cutPage(WorkJob job){
        List<Statistical> statisticals = new ArrayList<>();
        new JdbcUtils(jdbcTemplate)
                .query(String.format("select max(id) as max_num,min(id) as min_num,count(id) as count_num from %s where component_id = ?",job.getCurrentTable()),
                        Statistical.class,statisticals,job.getTask().getComponentCacheInfo().getComponentId()).error();

        if(CollectionUtils.isEmpty(statisticals)){
            saveESErrorLog(job.getTask().getComponentCacheInfo().getComponentId(), String.format("component.id = %s,table = %s 中没有数据",job.getTask().getComponentCacheInfo().getComponentId(),job.getCurrentTable()), "");
            return;
        }

        if(statisticals.get(0).countNum == 0){
            saveESErrorLog(job.getTask().getComponentCacheInfo().getComponentId(), String.format("component.id = %s,table = %s 中没有数据",job.getTask().getComponentCacheInfo().getComponentId(),job.getCurrentTable()), "");
            return;
        }

        //初始化缓存
        initLoadingCache(job);

        //加载el数据
        loadCacheData(job);



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
            job.getCount().incrementAndGet();
            getModelData(job,s,e);
        }

        //等待数据导入完成
        while(true){
            if(job.getCount().get() == 0){
                break;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(200L);
            }catch (Exception e){}
        }


        //清空缓存
        cacheInvalidateAll(job);
    }

    private void cacheInvalidateAll(WorkJob job){
        ConcurrentMap<String, ESCacheDTO> concurrentMap = job.getTask().getLoadingCache().asMap();

        List<FileFeatureES> fileFeatureES = new ArrayList<>();
        List<ComponentFileES> componentFileES = new ArrayList<>();
        List<FuncFeatureES> funcFeatureES = new ArrayList<>();

        concurrentMap.values().forEach(esCacheDTO->{
            if(Objects.isNull(esCacheDTO))return;

            if(esCacheDTO.getCacheFlag() == ESCacheDTO.DATE_BASE_FLAG_UN_SAVE_){
                synchronized (esCacheDTO){
                    if(esCacheDTO.getCacheFlag() == ESCacheDTO.DATE_BASE_FLAG_UN_SAVE_){
                        if (esCacheDTO.getCacheData() instanceof ComponentFileES){
                            ComponentFileES es = (ComponentFileES)esCacheDTO.getCacheData();
                            componentFileES.add(es);
                        }
                        if (esCacheDTO.getCacheData() instanceof FileFeatureES){
                            FileFeatureES es = (FileFeatureES)esCacheDTO.getCacheData();
                            fileFeatureES.add(es);
                        }
                        if (esCacheDTO.getCacheData() instanceof FuncFeatureES){
                            FuncFeatureES es = (FuncFeatureES)esCacheDTO.getCacheData();
                            funcFeatureES.add(es);
                        }

                        esCacheDTO.setCacheFlag(ESCacheDTO.DATE_BASE_FLAG_SAVE_);
                    }
                }
            }
        });

        try {
            if(!fileFeatureES.isEmpty()){
                fileFeatureDao.saveAll(fileFeatureES);
            }

            if(!componentFileES.isEmpty()){

            }

            if(!funcFeatureES.isEmpty()){

            }
        }catch (Exception e){
            saveESErrorLog(job.getTask().getComponentCacheInfo().getComponentId(), String.format("批量保存失败,%s",e.getMessage()), job.getCurrentTable());
        }
        job.getTask().getLoadingCache().invalidateAll();
    }

    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void getModelData(WorkJob job,Integer s,Integer e){
        try {
            List components = new ArrayList<>();
            Exception exception = new JdbcUtils(jdbcTemplate)
                .query(
                    String.format("select * from %s where id >= ? and id < ? and component_id = ?",job.getCurrentTable()),
                    job.getCurrentModelType().getEntityClass(),components,s,e,job.getTask().getComponentCacheInfo().getComponentId())
                .error();

            if (Objects.nonNull(exception)) {
                throw exception;
            }

            syncES(job,components);
        }catch (Exception ex){
            job.getCount().decrementAndGet();
            saveESErrorLog(job.getTask().getComponentCacheInfo().getComponentId(),"批量导出出错:"+ex.getMessage(),"");
        }
    }


    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void syncES(WorkJob job, List components){

        try{
            if(!CollectionUtils.isEmpty(components)) {
                for (Object item : components) {
                    ESCacheDTO dto = null;
                    Object data = null;
                    String key = "";
                    if(job.getCurrentModelType() == Constant.ModelType.MODEL_COMPONENT_FILE) {

                    }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FILE_FEATURE){
                        FileFeatureES es = new FileFeatureES((FileFeature) item);
                        key = es.getId();
                        data = es;
                    }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FUNC_FEATURE){

                    }

                    if(Strings.isNotBlank(key)) {
                        dto = getESCacheDTO(job, key);

                        if (Objects.isNull(dto)) {
                            dto = new ESCacheDTO(data, ESCacheDTO.DATE_BASE_FLAG_UN_SAVE_);
                            job.getTask().getLoadingCache().put(key, dto);
                        }
                    }
                }
            }
        }catch (Exception e) {
            saveESErrorLog(job.getTask().getComponentCacheInfo().getComponentId(),
                    String.format("%s 同步出错出错,%s",job.getCurrentTable(),e.getMessage()), job.getCurrentTable());
        }finally {
            job.getCount().decrementAndGet();
        }
    }


    //获取指定可以的数据
    private ESCacheDTO getESCacheDTO(WorkJob job,String key){
        return job.getTask().getLoadingCache().get(key, new Function<String, ESCacheDTO>() {
            @Override
            public ESCacheDTO apply(String fileHashValue) {

                Object data = null;
                if(job.getCurrentModelType() == Constant.ModelType.MODEL_COMPONENT_FILE) {

                }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FILE_FEATURE){
                    Optional<FileFeatureES> temp = fileFeatureDao.findById(key);
                    if (temp.isPresent()){
                        data = temp.get();
                    }
                }else if(job.getCurrentModelType() == Constant.ModelType.MODEL_FUNC_FEATURE){}

                return data == null ?
                        null:new ESCacheDTO(data,ESCacheDTO.DATE_BASE_FLAG_SAVE_);
            }
        });
    }

    private void dataExtract(WorkJob job){
        try{
            Map<Constant.ModelType,Object> configMap = job.getTask().getCfgMap();

            for(Constant.ModelType modelType : configMap.keySet()){

                //componentFile导入
                if(modelType == Constant.ModelType.MODEL_COMPONENT_FILE){
                    ComponentFileIndexES.setSuffix(modelType.getModelName());
                    Object temp = configMap.get(modelType);
                    String componentFileName = job.getTask().getComponentCacheInfo().getComponentFileName();

                    //设置当前模型
                    job.setCurrentModelType(modelType);

                    if(Objects.isNull(temp) &&
                            Strings.isNotBlank(componentFileName)){
                        //设置当前导出表
                        job.setCurrentTable(componentFileName);

                        //抽取指定表中的数据导入
                        cutPage(job);
                    }else{
                        //获取指定导出表信息
                        List<String> group = (List<String>)temp;
                        for(String item : group){
                            if(componentFileName.equals(modelType.getTableNameModel()+item)){
                                //设置当前导出表
                                job.setCurrentTable(componentFileName);

                                //抽取指定表中的数据导入
                                cutPage(job);
                            }
                        }
                    }
                }else{
                    //fileFeature，funcFeature数据导入
                    if(modelType == Constant.ModelType.MODEL_FILE_FEATURE){
                        FileFeatureIndexES.setSuffix(modelType.getModelName());
                    }

                    if(modelType == Constant.ModelType.MODEL_FUNC_FEATURE){
                        FuncFeatureIndexES.setSuffix(modelType.getModelName());
                    }

                    Object temp = configMap.get(modelType);

                    //设置当前模型
                    job.setCurrentModelType(modelType);

                    if(Objects.isNull(temp)){
                        for(Constant.LanguageType languageType : Constant.LanguageType.values()){
                            // 获取模型相关表信息
                            String value = getFieldValue(languageType,modelType,job);

                            if(Strings.isBlank(value)){
                                continue;
                            }

                            //设置当前导出表
                            job.setCurrentTable(value);

                            //设置当前语言
                            job.setCurrentLanguageType(languageType);

                            //抽取指定表中的数据导入
                            cutPage(job);
                        }
                    }else{
                        Map<String,List<String>> group = (Map<String,List<String>>)temp;
                        for(String item : group.keySet()){
                            //判断语言是否存在
                            Constant.LanguageType languageType = Constant.LanguageType.getLanguageTypeForAlias(item);
                            if(Objects.isNull(languageType)){
                                continue;
                            }

                            //获取
                            String value = getFieldValue(languageType,modelType,job);

                            if(Strings.isBlank(value)){
                                continue;
                            }

                            //设置当前语言
                            job.setCurrentLanguageType(languageType);

                            for(String num:group.get(item)){
                                String value_ = String.format(modelType.getTableNameModel(),languageType.getAlias())+num;
                                if(value.equals(value_)){
                                    //设置当前导出表
                                    job.setCurrentTable(value);

                                    //抽取指定表中的数据导入
                                    cutPage(job);
                                }
                            }
                        }
                    }
                }
            }
        }finally {
            engineComponent.curWorkJobCountDecrement();
        }
    }
}
