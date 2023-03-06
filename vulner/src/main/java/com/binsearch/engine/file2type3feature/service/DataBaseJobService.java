package com.binsearch.engine.file2type3feature.service;

import com.alibaba.fastjson.JSON;
import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.*;
import com.binsearch.engine.file2type3feature.FileType3FeatureEngineInfo;
import com.binsearch.engine.file2type3feature.Task;
import com.binsearch.engine.file2type3feature.WorkJob;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.orm.JdbcUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.binsearch.engine.file2type3feature.EngineConfiguration.*;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
public class DataBaseJobService {

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate transactionTemplate;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    @Qualifier(ETLConfiguration.ETL_JDBC_TRANSACTION_TEMPLATE)
    public DataSourceTransactionManager etlDataSourceTransactionManager;



    public List<Object> getComponentJobs(int s, int e, Task task) throws Exception {
        List<ComponentFile> componentFiles = new ArrayList<>();
        Exception exception = new JdbcUtils(jdbcTemplate)
                .query(String.format("select id,component_id,file_path,`language`,file_hash_value from "+task.getComponentCacheInfo().getComponentFileName()+" where language = '%s' and component_id = '%s' limit %d,%d",task.getLanguage(),task.getComponentCacheInfo().componentId,s,e),
                        ComponentFile.class,componentFiles).error();


        if(Objects.nonNull(exception))
            throw exception;

        List<Object> workJobs = new ArrayList<>();
        if(!CollectionUtils.isEmpty(componentFiles)) {
            for (ComponentFile componentFile : componentFiles) {
                workJobs.add(new WorkJob(task,componentFile));
            }
        }
        return workJobs;
    }

    public List<ComponentCacheInfo> getComponentCount(int s, int e) throws Exception{
        List<ComponentCacheInfo> componentCacheInfos = new ArrayList<>();
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .limit(s,e)
                .query(componentCacheInfos).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return componentCacheInfos;
    }

    public ComponentCacheInfo getComponentInfo(String componentId) throws Exception{
        List<ComponentCacheInfo> componentCacheInfos = new ArrayList<>();
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",componentId)
                .query(componentCacheInfos).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return componentCacheInfos.isEmpty()?null:componentCacheInfos.get(0);
    }


    public void updateComponentCacheFlag(String componentId,Object value) throws Exception{
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?",componentId)
                .update("file2type3feature_flag = ?",value).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }
    }

    public List<ComponentCacheInfo> getComponentCaches(String table) throws Exception{
        String sql = "select * from component_cache_ where component_file_name = '"+table+"' order by id";

        List<ComponentCacheInfo> componentCacheInfos = new ArrayList<>();
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .query(sql,ComponentCacheInfo.class,componentCacheInfos).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return componentCacheInfos;
    }





    public FileType3FeatureEngineInfo getFileFeatureEngineInfo() throws Exception {
        List<EngineStartInfo> engineStartInfos = new ArrayList<EngineStartInfo>(){};
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(EngineStartInfo.class)
                .where("engine_type = ?", FILE2TYPE3FEATURE_ENGINE)
                .query(engineStartInfos)
                .error();
        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return engineStartInfos.size() == 0 ? null: JSON.parseObject(engineStartInfos.get(0).getRunningInfo(), FileType3FeatureEngineInfo.class) ;
    }


    public void saveFileFeatureEngineInfo(FileType3FeatureEngineInfo record) {
        EngineStartInfo info = new EngineStartInfo();
        info.setEngineType(FILE2TYPE3FEATURE_ENGINE);
        info.setRunningInfo(JSON.toJSONString(record));

        new JdbcUtils(etlJdbcTemplate)
                .where("engine_type = ?", FILE2TYPE3FEATURE_ENGINE)
                .update(info);
    }

    public void saveFileFeatureErrorLog(String id,String log,String source,String target)throws Exception{
        EngineRunningError error = new EngineRunningError();
        error.setComponentId(id);
        error.setErrorDate(new Timestamp(System.currentTimeMillis()));
        error.setRunningErrorInfo(log);
        error.setTargetTable(target);
        error.setSourceTable(source);
        error.setEngineType(FILE2TYPE3FEATURE_ENGINE);

        Exception exception = new JdbcUtils(etlJdbcTemplate).create(error).error();
        if(Objects.nonNull(exception)){
            throw exception;
        }
    }





    public void createFileType3Feature(String language,int count) {
        String[] temp = {
            "CREATE TABLE `t_sourcecode_%s_filetype3feature%d` (" +
                "`id` bigint(12) NOT NULL AUTO_INCREMENT," +
                "`component_file_id` bigint(12) NOT NULL,"+
                "`component_id` char(100) NOT NULL,"+
                "`types` mediumtext," +
                "`type1` varchar(50) DEFAULT NULL," +
                "`type2` varchar(50) DEFAULT NULL," +
                "`type3` varchar(50) DEFAULT NULL," +
                "`type4` varchar(50) DEFAULT NULL," +
                "`type5` varchar(50) DEFAULT NULL," +
                "`type6` varchar(50) DEFAULT NULL," +
                "`type7` varchar(50) DEFAULT NULL," +
                "`type8` varchar(50) DEFAULT NULL," +
                "`type9` varchar(50) DEFAULT NULL," +
                "`type10` varchar(50) DEFAULT NULL," +
                "`type11` varchar(50) DEFAULT NULL," +
                "`type12` varchar(50) DEFAULT NULL," +
                "`type13` varchar(50) DEFAULT NULL," +
                "`type14` varchar(50) DEFAULT NULL," +
                "`type15` varchar(50) DEFAULT NULL," +
                "`type16` varchar(50) DEFAULT NULL," +
                "`type17` varchar(50) DEFAULT NULL," +
                "`type18` varchar(50) DEFAULT NULL," +
                "`type19` varchar(50) DEFAULT NULL," +
                "`type20` varchar(50) DEFAULT NULL," +
                "`type21` varchar(50) DEFAULT NULL," +
                "`type22` varchar(50) DEFAULT NULL," +
                "`type23` varchar(50) DEFAULT NULL," +
                "`type24` varchar(50) DEFAULT NULL," +
                "`type25` varchar(50) DEFAULT NULL," +
                "`type26` varchar(50) DEFAULT NULL," +
                "`type27` varchar(50) DEFAULT NULL," +
                "`type28` varchar(50) DEFAULT NULL," +
                "`type29` varchar(50) DEFAULT NULL," +
                "`type30` varchar(50) DEFAULT NULL," +
                "`type31` varchar(50) DEFAULT NULL," +
                "`type32` varchar(50) DEFAULT NULL," +
                "`type33` varchar(50) DEFAULT NULL," +
                "`type34` varchar(50) DEFAULT NULL," +
                "`type35` varchar(50) DEFAULT NULL," +
                "`type36` varchar(50) DEFAULT NULL," +
                "`type37` varchar(50) DEFAULT NULL," +
                "`type38` varchar(50) DEFAULT NULL," +
                "`type39` varchar(50) DEFAULT NULL," +
                "`type40` varchar(50) DEFAULT NULL," +
                "`type41` varchar(50) DEFAULT NULL," +
                "`type42` varchar(50) DEFAULT NULL," +
                "`type43` varchar(50) DEFAULT NULL," +
                "`type44` varchar(50) DEFAULT NULL," +
                "`type45` varchar(50) DEFAULT NULL," +
                "`type46` varchar(50) DEFAULT NULL," +
                "`type47` varchar(50) DEFAULT NULL," +
                "`type48` varchar(50) DEFAULT NULL," +
                "`type49` varchar(50) DEFAULT NULL," +
                "`type50` varchar(50) DEFAULT NULL," +
                "`type51` varchar(50) DEFAULT NULL," +
                "`type52` varchar(50) DEFAULT NULL," +
                "`type53` varchar(50) DEFAULT NULL," +
                "`type54` varchar(50) DEFAULT NULL," +
                "`type55` varchar(50) DEFAULT NULL," +
                "`type56` varchar(50) DEFAULT NULL," +
                "`type57` varchar(50) DEFAULT NULL," +
                "`type58` varchar(50) DEFAULT NULL," +
                "`type59` varchar(50) DEFAULT NULL," +
                "`type60` varchar(50) DEFAULT NULL," +
                "`source_table` varchar(255) DEFAULT NULL," +
                "`create_date` timestamp," +
                "PRIMARY KEY (`id`)," +
                "KEY `t_file_feature_component_id` (`component_id`),",
                "KEY `t_file_feature_component_file_id` (`component_file_id`),",
                "KEY `t_file_feature_component_create_date` (`create_date`),",
                "KEY `t_file_feature_component_create_type1` (`type1`),",
                "KEY `t_file_feature_component_create_type2` (`type2`),",
                "KEY `t_file_feature_component_create_type3` (`type3`),",
                "KEY `t_file_feature_component_create_type4` (`type4`),",
                "KEY `t_file_feature_component_create_type5` (`type5`),",
                "KEY `t_file_feature_component_create_type6` (`type6`),",
                "KEY `t_file_feature_component_create_type7` (`type7`),",
                "KEY `t_file_feature_component_create_type8` (`type8`),",
                "KEY `t_file_feature_component_create_type9` (`type9`),",
                "KEY `t_file_feature_component_create_type10` (`type10`),",
                "KEY `t_file_feature_component_create_type11` (`type11`),",
                "KEY `t_file_feature_component_create_type12` (`type12`),",
                "KEY `t_file_feature_component_create_type13` (`type13`),",
                "KEY `t_file_feature_component_create_type14` (`type14`),",
                "KEY `t_file_feature_component_create_type15` (`type15`),",
                "KEY `t_file_feature_component_create_type16` (`type16`),",
                "KEY `t_file_feature_component_create_type17` (`type17`),",
                "KEY `t_file_feature_component_create_type18` (`type18`),",
                "KEY `t_file_feature_component_create_type19` (`type19`),",
                "KEY `t_file_feature_component_create_type20` (`type20`),",
                "KEY `t_file_feature_component_create_type21` (`type21`),",
                "KEY `t_file_feature_component_create_type22` (`type22`),",
                "KEY `t_file_feature_component_create_type23` (`type23`),",
                "KEY `t_file_feature_component_create_type24` (`type24`),",
                "KEY `t_file_feature_component_create_type25` (`type25`),",
                "KEY `t_file_feature_component_create_type26` (`type26`),",
                "KEY `t_file_feature_component_create_type27` (`type27`),",
                "KEY `t_file_feature_component_create_type28` (`type28`),",
                "KEY `t_file_feature_component_create_type29` (`type29`),",
                "KEY `t_file_feature_component_create_type30` (`type30`),",
                "KEY `t_file_feature_component_create_type31` (`type31`),",
                "KEY `t_file_feature_component_create_type32` (`type32`),",
                "KEY `t_file_feature_component_create_type33` (`type33`),",
                "KEY `t_file_feature_component_create_type34` (`type34`),",
                "KEY `t_file_feature_component_create_type35` (`type35`),",
                "KEY `t_file_feature_component_create_type36` (`type36`),",
                "KEY `t_file_feature_component_create_type37` (`type37`),",
                "KEY `t_file_feature_component_create_type38` (`type38`),",
                "KEY `t_file_feature_component_create_type39` (`type39`),",
                "KEY `t_file_feature_component_create_type40` (`type40`),",
                "KEY `t_file_feature_component_create_type41` (`type41`),",
                "KEY `t_file_feature_component_create_type42` (`type42`),",
                "KEY `t_file_feature_component_create_type43` (`type43`),",
                "KEY `t_file_feature_component_create_type44` (`type44`),",
                "KEY `t_file_feature_component_create_type45` (`type45`),",
                "KEY `t_file_feature_component_create_type46` (`type46`),",
                "KEY `t_file_feature_component_create_type47` (`type47`),",
                "KEY `t_file_feature_component_create_type48` (`type48`),",
                "KEY `t_file_feature_component_create_type49` (`type49`),",
                "KEY `t_file_feature_component_create_type50` (`type50`),",
                "KEY `t_file_feature_component_create_type51` (`type51`),",
                "KEY `t_file_feature_component_create_type52` (`type52`),",
                "KEY `t_file_feature_component_create_type53` (`type53`),",
                "KEY `t_file_feature_component_create_type54` (`type54`),",
                "KEY `t_file_feature_component_create_type55` (`type55`),",
                "KEY `t_file_feature_component_create_type56` (`type56`),",
                "KEY `t_file_feature_component_create_type57` (`type57`),",
                "KEY `t_file_feature_component_create_type58` (`type58`),",
                "KEY `t_file_feature_component_create_type59` (`type59`),",
                "KEY `t_file_feature_component_create_type60` (`type60`),",
                "KEY `t_file_feature_component_id_file_id` (`component_id`,`component_file_id`)",
            ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"
        };

        for(int i=1; i<=count; i++) {
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .model(FileType3Feature.class)
                    .table(String.format("t_sourcecode_%s_filetype3feature%d", language, i))
                    .limit(1)
                    .query(new ArrayList<FileFeature>())
                    .error();

            if(Objects.nonNull(exception)) {
                jdbcTemplate.update(String.format(StringUtils.join(temp), language, i));
            }
        }
    }
}
