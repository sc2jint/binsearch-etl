package com.binsearch.engine.component2file.service;

import com.alibaba.fastjson.JSON;
import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.component2file.*;
import com.binsearch.engine.entity.db.*;
import com.binsearch.etl.orm.EntityCountNum;
import com.binsearch.etl.orm.JdbcUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.util.*;

import static com.binsearch.engine.component2file.Engine.SOURCE_TYPE_GIT;
import static com.binsearch.engine.component2file.Engine.SOURCE_TYPE_MAVEN;
import static com.binsearch.engine.component2file.EngineConfiguration.*;
import static com.binsearch.etl.ETLConfiguration.*;

public class DataBaseJobService {

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_SOURCE)
    JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_SOURCE)
    TransactionTemplate transactionTemplate;

    @Value("${base.system.source.path.git}")
    private String gitSourceFileRootPath;

    @Value("${base.system.source.path.maven}")
    private String mavenSourceFileRootPath;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate targetJdbctemplat;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate targetTransactionTemplate;

    @Autowired
    @Qualifier(ETL_JDBC_TEMPLATE)
    JdbcTemplate etlJdbcTemplate;

    @Autowired
    @Qualifier(ETL_JDBC_TRANSACTION_TEMPLATE)
    public DataSourceTransactionManager etlDataSourceTransactionManager;



    //获取git上的任务
    public List getViewTasks(int s, int e) throws Exception {
        List<ViewTasks> viewTasks = new ArrayList<ViewTasks>(){};
        Exception exception = new JdbcUtils(jdbcTemplate).model(ViewTasks.class)
                .limit(s,e).query(viewTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
        return viewTasks;
    }

    //获取maven上的任务
    public List getMavenTasks(int s, int e) throws Exception {
        List<MavenTasks> mavenTasks = new ArrayList<MavenTasks>(){};
        Exception exception = new JdbcUtils(jdbcTemplate).model(MavenTasks.class)
                .limit(s,e).query(mavenTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
        return mavenTasks;
    }


    public List<ViewTasks> getViewTasks(String id) throws Exception {
        LinkedList<ViewTasks> viewTasks = new LinkedList<ViewTasks>(){};
        Exception exception = new JdbcUtils(jdbcTemplate).model(ViewTasks.class)
                .where("task_id = ?",id).query(viewTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
        return viewTasks;
    }


    public List getViewDetailTasks(ViewTasks viewTask) throws Exception {
        List<ViewDetailTasks> viewDetailTasks = new ArrayList<ViewDetailTasks>() {};
        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(ViewDetailTasks.class)
                .where("task_id = ?", viewTask.taskId)
                .query(viewDetailTasks).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }

        return viewDetailTasks;
    }

    public List getMavenDetailTasks(MavenTasks mavenTasks) throws Exception {
        List<MavenDetailTasks> mavenDetailTasks = new ArrayList<MavenDetailTasks>() {};
        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(MavenDetailTasks.class)
                .where("compo_id = ?", mavenTasks.compoId)
                .query(mavenDetailTasks).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }

        return mavenDetailTasks;
    }


    public ComponentCacheInfo getComponentCache(String componentId)throws Exception{
        ArrayList<ComponentCacheInfo> componentCacheInfos = new ArrayList<ComponentCacheInfo>() {};
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(ComponentCacheInfo.class)
                .where("component_id = ?", componentId)
                .query(componentCacheInfos).error();

        if (Objects.nonNull(exception)) {
            throw exception;
        }
        return componentCacheInfos.isEmpty()?null:componentCacheInfos.get(0);
    }




    public void saveComponentCache(ComponentCacheInfo info)throws Exception{
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .create(info).error();
        if (Objects.nonNull(exception)) {
            throw exception;
        }
    }

    // 获取git主键任务
    // 118:[v7.6.0.zip,...]@...格式
    public List<Object> getGitComponentJobs(String component)throws Exception{
        List<Object> workJobs = new ArrayList<Object>(){};
        LinkedList<ViewTasks> viewTasks = new LinkedList<ViewTasks>(){};
        //获取组件ID
        String[] componentInfo = component.split(":");

        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(ViewTasks.class)
                .where("task_id = ?",componentInfo[0])
                .query(viewTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }

        for(ViewTasks viewTask:viewTasks) {
            LinkedList<ViewDetailTasks> viewDetailTasks = new LinkedList<ViewDetailTasks>() {};

            String sql = "select vdt.* from view_detail_tasks vdt,view_tasks vt where vt.task_id = ? and vdt.task_id = vt.task_id";

            if (componentInfo.length > 1) {
                //获取指定版本信息
                String[] info = componentInfo[1].split(",");
                for(int i = 0;i < info.length;i++){
                    info[i] = info[i].replace("'","\\'")
                            .replace("[","")
                            .replace("]","");
                    info[i] = String.format("'%s'",info[i]);
                }

                sql += String.format(" and vdt.version_name in(%s)",StringUtils.join(info,","));
            }
            exception = new JdbcUtils(jdbcTemplate)
                    .query(sql, ViewDetailTasks.class, viewDetailTasks, viewTask.taskId).error();

            if (Objects.nonNull(exception)) {
                throw exception;
            }

            //检查缓存表,同步新组件
            ComponentCacheInfo componentCacheInfo = getComponentCache(componentInfo[0]);
            if (Objects.isNull(componentCacheInfo)) {
                componentCacheInfo = new ComponentCacheInfo();
                componentCacheInfo.setComponentId(componentInfo[0]);
                componentCacheInfo.setComponent2fileFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFunc2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2type3featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setCodeSource(SOURCE_TYPE_GIT);
                componentCacheInfo.setMainLanguage(viewTask.mainLang);
                saveComponentCache(componentCacheInfo);
            }

            //创建任务
            Task task = new Task(viewTask, viewDetailTasks.size(),this,componentCacheInfo);
            for (ViewDetailTasks viewDetailTask : viewDetailTasks) {
                workJobs.add(new WorkJob(task, viewDetailTask,SOURCE_TYPE_GIT,gitSourceFileRootPath));
            }
        }
        return workJobs;
    }

    public List<Object> getMavenComponentJobs(String component)throws Exception{
        List<Object> workJobs = new ArrayList<Object>(){};
        LinkedList<MavenTasks> mavenTasks = new LinkedList<MavenTasks>(){};
        //获取组件ID
        String[] componentInfo = component.split(":");

        Exception exception = new JdbcUtils(jdbcTemplate)
                .model(MavenTasks.class)
                .where("compo_id = ?",componentInfo[0])
                .query(mavenTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }

        for(MavenTasks mavenTask:mavenTasks) {
            List<MavenDetailTasks> mavenDetailTasks = new ArrayList<MavenDetailTasks>() {};

            String sql = "select vdt.* from maven_detail_task vdt,maven_task vt where vt.compo_id = ? and vdt.compo_id = vt.compo_id and vdt.source_code_status_code = 200 and vdt.create_time > str_to_date('2021-12-01', '%Y-%m-%d %H:%i:%S')";

            if(componentInfo.length > 1) {
                //获取指定版本信息
                String[] info = componentInfo[1].split(",");
                for(int i = 0;i < info.length;i++){
                    info[i] = info[i].replace("'","\\'")
                            .replace("[","")
                            .replace("]","");
                    info[i] = String.format("'%s'",info[i]);
                }

                sql += String.format(" and vdt.current_version in(%s)",StringUtils.join(info,","));
            }
            exception = new JdbcUtils(jdbcTemplate)
                    .query(sql, MavenDetailTasks.class, mavenDetailTasks, mavenTask.compoId).error();

            if (Objects.nonNull(exception)) {
                throw exception;
            }

            //检查缓存表,同步新组件
            ComponentCacheInfo componentCacheInfo = getComponentCache(componentInfo[0]);
            if (Objects.isNull(componentCacheInfo)) {
                componentCacheInfo = new ComponentCacheInfo();
                componentCacheInfo.setComponentId(componentInfo[0]);
                componentCacheInfo.setComponent2fileFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFunc2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2type3featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setCodeSource(SOURCE_TYPE_MAVEN);
                componentCacheInfo.setMainLanguage("JAVA");
                saveComponentCache(componentCacheInfo);
            }

            //创建任务
            Task task = new Task(mavenTask, mavenDetailTasks.size(),this,componentCacheInfo);
            for (MavenDetailTasks mavenDetailTask : mavenDetailTasks) {
                workJobs.add(new WorkJob(task, mavenDetailTask,SOURCE_TYPE_MAVEN,mavenSourceFileRootPath));
            }
        }
        return workJobs;
    }



    public void saveComponentErrorLog(String id,String log,String target)throws Exception{
        EngineRunningError error = new EngineRunningError();
        error.setComponentId(id);
        error.setErrorDate(new Timestamp(System.currentTimeMillis()));
        error.setRunningErrorInfo(log);
        error.setTargetTable(target);
        error.setEngineType(COMPONENT2FILE_ENGINE);

        Exception exception = new JdbcUtils(etlJdbcTemplate).create(error).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
    }

    /**
     * 获取分页组件数据
     * */
    public List<Object> getGitComponentJobs(int s, int e)throws Exception{
        List<Object> workJobs = new ArrayList<Object>(){};
        LinkedList<ViewTasks> viewTasks = new LinkedList<ViewTasks>(){};
        Exception exception = new JdbcUtils(jdbcTemplate).model(ViewTasks.class)
                .limit(s,e).query(viewTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
        for(ViewTasks viewTask:viewTasks){
            //获取版本信息
            LinkedList<ViewDetailTasks> viewDetailTasks = new LinkedList<ViewDetailTasks>(){};
            exception = new JdbcUtils(jdbcTemplate).query("select vdt.* from view_detail_tasks vdt,view_tasks vt where vt.task_id = ? and vdt.task_id = vt.task_id",ViewDetailTasks.class,viewDetailTasks,viewTask.taskId).error();
            if(Objects.nonNull(exception)){
                throw exception;
            }

            //检查缓存表,插入组件
            ComponentCacheInfo componentCacheInfo = getComponentCache(viewTask.taskId);
            if (Objects.isNull(componentCacheInfo)) {
                componentCacheInfo = new ComponentCacheInfo();
                componentCacheInfo.setComponentId(viewTask.taskId);
                componentCacheInfo.setComponent2fileFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFunc2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2type3featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setCodeSource(SOURCE_TYPE_GIT);
                componentCacheInfo.setMainLanguage(viewTask.mainLang);
                saveComponentCache(componentCacheInfo);
            }

            //生成任务
            Task task = new Task(viewTask,viewDetailTasks.size(),this,componentCacheInfo);
            for(ViewDetailTasks viewDetailTask:viewDetailTasks){
                workJobs.add(new WorkJob(task,viewDetailTask,SOURCE_TYPE_GIT,gitSourceFileRootPath));
            }
        }
        return workJobs;
    }

    public List<Object> getMavenComponentJobs(int s, int e)throws Exception{
        List<Object> workJobs = new ArrayList<Object>(){};
        List<MavenTasks> mavenTasks = new ArrayList<MavenTasks>(){};
        Exception exception = new JdbcUtils(jdbcTemplate).model(MavenTasks.class)
                .limit(s,e).query(mavenTasks).error();

        if(Objects.nonNull(exception)){
            throw exception;
        }
        for(MavenTasks mavenTask:mavenTasks){
            //获取版本信息
            List<MavenDetailTasks> mavenDetailTasks = new ArrayList<MavenDetailTasks>(){};
            exception = new JdbcUtils(jdbcTemplate).query("select vdt.* from maven_detail_task vdt,maven_task vt where vt.compo_id = ? and vdt.compo_id = vt.compo_id and vdt.source_code_status_code = 200 and vdt.create_time > str_to_date('2021-12-01', '%Y-%m-%d %H:%i:%S')",
                    MavenDetailTasks.class,mavenDetailTasks,mavenTask.compoId).error();
            if(Objects.nonNull(exception)){
                throw exception;
            }

            //检查缓存表,插入组件
            ComponentCacheInfo componentCacheInfo = getComponentCache(mavenTask.compoId);
            if (Objects.isNull(componentCacheInfo)) {
                componentCacheInfo = new ComponentCacheInfo();
                componentCacheInfo.setComponentId(mavenTask.compoId);
                componentCacheInfo.setComponent2fileFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFunc2featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setFile2type3featureFlag(ComponentCacheInfo.RUNNING_FLAGS_UN_END);
                componentCacheInfo.setCodeSource(SOURCE_TYPE_MAVEN);
                componentCacheInfo.setMainLanguage("JAVA");
                saveComponentCache(componentCacheInfo);
            }

            //生成任务
            Task task = new Task(mavenTask,mavenDetailTasks.size(),this,componentCacheInfo);
            for(MavenDetailTasks mavenDetailTask:mavenDetailTasks){
                workJobs.add(new WorkJob(task,mavenDetailTask,SOURCE_TYPE_MAVEN,mavenSourceFileRootPath));
            }
        }
        return workJobs;
    }

    /**
     * 获取组件总数
     * */
    public int getGitComponentCount()throws Exception{
        EntityCountNum num = new EntityCountNum();
        Exception exception = new JdbcUtils(jdbcTemplate).model(ViewTasks.class).count(num).error();
        if (Objects.nonNull(exception)){
            throw exception;
        }
        return num.getCountNum();
    }

    public int getMavenComponentCount()throws Exception{
        EntityCountNum num = new EntityCountNum();
        Exception exception = new JdbcUtils(jdbcTemplate).model(MavenTasks.class).count(num).error();
        if (Objects.nonNull(exception)){
            throw exception;
        }
        return num.getCountNum();
    }

    public ComponentFileEngineInfo getComponenFileEngineInfo()throws Exception{
        List<EngineStartInfo> engineStartInfos = new ArrayList<EngineStartInfo>(){};
        Exception exception = new JdbcUtils(etlJdbcTemplate)
                .model(EngineStartInfo.class)
                .where("engine_type = ?", COMPONENT2FILE_ENGINE)
                .query(engineStartInfos)
                .error();

        if (Objects.nonNull(exception)) {
            throw new Exception("BinsearchEngine 信息获取失败"+exception.getMessage());
        }
        return JSON.parseObject(engineStartInfos.get(0).runningInfo,ComponentFileEngineInfo.class);
    }

    public void saveComponenFileEngineInfo(ComponentFileEngineInfo fileEngineInfo){
        EngineStartInfo info = new EngineStartInfo();
        info.setEngineType(COMPONENT2FILE_ENGINE);
        info.setRunningInfo(JSON.toJSONString(fileEngineInfo));

        new JdbcUtils(etlJdbcTemplate)
                .where("engine_type = ?", COMPONENT2FILE_ENGINE)
                .update(info);
    }

    public void creatComponentFile(Integer tableCount)throws Exception{
        String[] temp = {
                "CREATE TABLE `t_component_file%s` (",
                "`id` bigint(12) NOT NULL AUTO_INCREMENT,",
                "`component_id` char(100) DEFAULT NULL,",
                "`source_file_path` mediumtext,",
                "`file_path` char(150) DEFAULT NULL,",
                "`language` char(50) DEFAULT NULL,",
                "`component_version_id` mediumtext,",
                "`file_hash_value` char(255) DEFAULT NULL,",
                "PRIMARY KEY (`id`),",
                "KEY `t_file_component_id` (`component_id`),",
                "KEY `t_file_hash_value` (`file_hash_value`),",
                "KEY `t_component_file_hash_value` (`component_id`,`file_hash_value`),",
                "KEY `t_language` (`language`)",
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;",
        };

        for(int i = 1;i <= tableCount;i++){
            Exception exception = new JdbcUtils(targetJdbctemplat)
                    .model(ComponentFile.class)
                    .table(String.format("t_component_file%s",i))
                    .limit(1)
                    .query(new ArrayList<ComponentFile>())
                    .error();
            if(Objects.nonNull(exception)){
                targetJdbctemplat.update(String.format(StringUtils.join(temp),i));
            }
        }
    }
}