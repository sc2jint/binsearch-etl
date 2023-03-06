package com.binsearch.engine.file2feature.service;

import com.alibaba.fastjson.JSON;
import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.*;
import com.binsearch.engine.file2feature.*;
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

import static com.binsearch.engine.file2feature.EngineConfiguration.*;

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
                .update("file2feature_flag = ?",value).error();

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





    public FileFeatureEngineInfo getFileFeatureEngineInfo() throws Exception {
        List<EngineStartInfo> engineStartInfos = new ArrayList<EngineStartInfo>(){};
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(EngineStartInfo.class)
                .where("engine_type = ?", FILE2FEATURE_ENGINE)
                .query(engineStartInfos)
                .error();
        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return engineStartInfos.size() == 0 ? null: JSON.parseObject(engineStartInfos.get(0).getRunningInfo(),FileFeatureEngineInfo.class) ;
    }


    public void saveFileFeatureEngineInfo(FileFeatureEngineInfo record) {
        EngineStartInfo info = new EngineStartInfo();
        info.setEngineType(FILE2FEATURE_ENGINE);
        info.setRunningInfo(JSON.toJSONString(record));

        new JdbcUtils(etlJdbcTemplate)
                .where("engine_type = ?", FILE2FEATURE_ENGINE)
                .update(info);
    }

    public void saveFileFeatureErrorLog(String id,String log,String source,String target)throws Exception{
        EngineRunningError error = new EngineRunningError();
        error.setComponentId(id);
        error.setErrorDate(new Timestamp(System.currentTimeMillis()));
        error.setRunningErrorInfo(log);
        error.setTargetTable(target);
        error.setSourceTable(source);
        error.setEngineType(FILE2FEATURE_ENGINE);

        Exception exception = new JdbcUtils(etlJdbcTemplate).create(error).error();
        if(Objects.nonNull(exception)){
            throw exception;
        }
    }





    public void createFileFeature(String language,int count) {
        String[] temp = {
            "CREATE TABLE `t_sourcecode_%s_filefeature%d` (" +
                "`id` bigint(12) NOT NULL AUTO_INCREMENT," +
                "`component_file_id` bigint(12) NOT NULL,"+
                "`component_id` char(100) NOT NULL," +
                "`line_number` int DEFAULT NULL," +
                "`token_number` int DEFAULT NULL," +
                "`type0` varchar(255) DEFAULT NULL," +
                "`type1` varchar(255) DEFAULT NULL," +
                "`type2blind` varchar(255) DEFAULT NULL," +
                "`source_table` varchar(255) DEFAULT NULL," +
                "`create_date` timestamp," +
                "PRIMARY KEY (`id`)," +
                "KEY `t_file_feature_component_id` (`component_id`),",
                "KEY `t_file_feature_component_file_id` (`component_file_id`),",
                "KEY `t_file_feature_component_create_date` (`create_date`),",
                "KEY `t_file_feature_component_type0` (`type0`),",
                "KEY `t_file_feature_component_type1` (`type1`),",
                "KEY `t_file_feature_type2blind` (`type2blind`),",
                "KEY `t_file_feature_component_source_table` (`source_table`),",
                "KEY `t_file_feature_component_id_file_id` (`component_id`,`component_file_id`)",
            ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"
        };

        for(int i=1; i<=count; i++) {
            Exception exception = new JdbcUtils(jdbcTemplate)
                    .model(FileFeature.class)
                    .table(String.format("t_sourcecode_%s_filefeature%d", language, i))
                    .limit(1)
                    .query(new ArrayList<FileFeature>())
                    .error();

            if(Objects.nonNull(exception)) {
                jdbcTemplate.update(String.format(StringUtils.join(temp), language, i));
            }
        }
    }
}
