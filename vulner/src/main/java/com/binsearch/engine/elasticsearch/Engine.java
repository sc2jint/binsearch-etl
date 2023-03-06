package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.Constant;
import com.binsearch.engine.ETLService;

import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.entity.el.index.ComponentFileIndexES;
import com.binsearch.etl.EngineComponent;

import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.binsearch.etl.ETLConfiguration.ETL_JDBC_TEMPLATE;

public class Engine implements ETLService {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(Engine.class);


    @Autowired
    @Qualifier(ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    //模型类型
    @Value("#{'${elasticsearch.fullComponent.data.model.type}'.split('@')}")
    List<String> dataModelType;


    @Value("#{'${elasticsearch.fullComponent.data.component.languages}'.split(',')}")
    List<String> componentLanguages;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(EngineConfiguration.ELASTICSEARCH_EXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(EngineConfiguration.ELASTICSEARCH_CLIENT)
    private ElasticsearchRestTemplate elTemplate;



    private Map<Constant.ModelType,Object> cfgMap;


    @Override
    public void init() throws Exception {
        if(CollectionUtils.isEmpty(dataModelType)){
            throw new Exception("ELASTICSEARCH_ENGINE dataModelType 不能为空");
        }

        if(Objects.isNull(cfgMap)){
            cfgMap = new HashMap<Constant.ModelType,Object>();

            for(String type:dataModelType){
                //是否指定范围
                if(type.contains(":")) {
                    String temp[] = type.split(":");
                    temp[1] = temp[1].substring(1,temp[1].length()-1);

                    //解析获取componentFile类型
                    Constant.ModelType model = Constant.ModelType.MODEL_COMPONENT_FILE;

                    //解析获取fileFeature类型
                    if (type.contains(Constant.ModelType.MODEL_FILE_FEATURE.getModelName())) {
                        model = Constant.ModelType.MODEL_FILE_FEATURE;
                    }

                    //解析获取funcFeature类型
                    if (type.contains(Constant.ModelType.MODEL_FUNC_FEATURE.getModelName())) {
                         model = Constant.ModelType.MODEL_FUNC_FEATURE;
                    }

                    if(Objects.nonNull(model)){
                        if(model == Constant.ModelType.MODEL_COMPONENT_FILE){
                            cfgMap.put(model, new ArrayList<String>());

                            temp = temp[1].split(",");
                            for (String s : temp) {
                                ((ArrayList) cfgMap.get(model)).add(s);
                            }
                        }else {
                            Map<String, List<String>> map = new HashMap<String, List<String>>();

                            //匹配分组
                            Pattern pattern = Pattern.compile("\\{\\w+&\\[{1}(\\w|\\,)+\\]{1}\\}");
                            Matcher matcher = pattern.matcher(temp[1]);
                            while (matcher.find()) {
                                //获取分组
                                String tempstr = matcher.group().trim();
                                tempstr = tempstr.replaceAll("\\{|\\}", "");

                                //解析分组
                                String[] groups = tempstr.split("&");
                                map.put(groups[0], new ArrayList<String>());

                                //获取分组信息
                                String[] items = groups[1].replaceAll("\\[|\\]","").split(",");
                                for (String item:items) {
                                    ((ArrayList)map.get(groups[0])).add(item);
                                }
                            }
                            cfgMap.put(model,map);
                        }
                    }
                }else{
                    cfgMap.put(Constant.ModelType.getModelType(type),null);
                }
            }
        }

        if(cfgMap.isEmpty()){
            throw new Exception("ELASTICSEARCH_ENGINE 配置信息为空");
        }

        //检查el索引
        if(Objects.nonNull(elTemplate)){
            for(Constant.ModelType modelType:cfgMap.keySet()){
                if(modelType == Constant.ModelType.MODEL_COMPONENT_FILE){
                    //索引检查

                    if(!elTemplate.indexExists(modelType.getModelName())){
                        elTemplate.createIndex(modelType.getModelName());
                    }
                }else{
                    Object value = cfgMap.get(modelType);
                    //判断是否指定范围
                    if(!Objects.isNull(value)){
                        Map<String, List<String>> temp = (HashMap<String, List<String>>)value;
                        for (String s : temp.keySet()) {
                            if(Objects.isNull(Constant.LanguageType.getLanguageTypeForAlias(s))){
                                throw new Exception("ELASTICSEARCH_ENGINE 未识别的配置语言,"+s);
                            }
                        }
                    }

                    if (elTemplate.indexExists(modelType.getModelName())) {
                        elTemplate.createIndex(modelType.getModelName());
                    }
                }
            }
        }
    }

    //根据组件进行比较
    @Override
    public void incrementalComponent() {

    }

    @Override
    public void specifyComponent() {

    }

    @Override
    public void fullComponent() {
        int pageSize = 100;//每页数量
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
                        extractJobService.dataExtract();
                    }
                    break;
                }


                for(ComponentCacheInfo cache:caches) {
                    if(componentLanguages.contains(cache.getMainLanguage())){
                        Task task = new Task();
                        task.setCfgMap(cfgMap);
                        task.setComponentCacheInfo(cache);

                        WorkJob workJob = new WorkJob();
                        workJob.setTask(task);
                        engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJob(workJob);
                    }
                }

                engineComponent.setCurWorkJobCount(caches.size());

                while(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                    extractJobService.dataExtract();
                }

            }catch (Exception e){}
            finally{
                unResources(engineComponent);
                curPage++;
            }
        }
        unResources(engineComponent);
    }

    public int getComponentCacheInfoCount(){
        EntityCountNum num = new EntityCountNum();
        new JdbcUtils(etlJdbcTemplate).model(ComponentCacheInfo.class).count(num);
        return num.getCountNum();
    }
}
