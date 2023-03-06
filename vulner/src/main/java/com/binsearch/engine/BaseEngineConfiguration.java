package com.binsearch.engine;

import com.binsearch.engine.func2feature.Engine;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.binsearch.engine.BaseEngineConfiguration.BASE_CONFIGURATION;

//读取配置文件
@PropertySource(value = {"classpath:engine/engineConfiguration.properties"})
@Configuration(value = BASE_CONFIGURATION)
public class BaseEngineConfiguration {

    public static final String BASE_CONFIGURATION = "BASE_CONFIGURATION";

    ////////////////////////////////////////////////////////
    @Bean
    public BaseEngine engine() {
        return new BaseEngine();
    }


    public static final String BASE_MYSQL_DATA_SOURCE = "BASE_MYSQL_DATA_SOURCE";
    public static final String BASE_MYSQL_JDBC_TEMPLATE_SOURCE = "BASE_MYSQL_JDBC_TEMPLATE_SOURCE";
    public static final String BASE_MYSQL_JDBC_TRANSACTION_SOURCE = "BASE_MYSQL_JDBC_TRANSACTION_SOURCE";

    @Bean(BASE_MYSQL_DATA_SOURCE)
    @ConfigurationProperties(prefix = "spring.datasource.source")
    public DataSource newDataSource1(){
       return new HikariDataSource();
    }

    @Bean(BASE_MYSQL_JDBC_TEMPLATE_SOURCE)
    public JdbcTemplate newJdbcTemplate1(@Qualifier(BASE_MYSQL_DATA_SOURCE) DataSource dataSource){
       return new JdbcTemplate(dataSource);
    }

    @Bean(BASE_MYSQL_JDBC_TRANSACTION_SOURCE)
    public TransactionTemplate transactionTemplate1(@Qualifier(BASE_MYSQL_DATA_SOURCE)DataSource dataSource){
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
        return new TransactionTemplate(dataSourceTransactionManager);
    }


    ////////////////////////////////////////////////////////


    public static final String BASE_MYSQL_DATA_TARGET = "BASE_MYSQL_DATA_TARGET";
    public static final String BASE_MYSQL_JDBC_TEMPLATE_TARGET = "BASE_MYSQL_JDBC_TEMPLATE_TARGET";
    public static final String BASE_MYSQL_JDBC_TRANSACTION_TARGET = "BASE_MYSQL_JDBC_TRANSACTION_TARGET";

    @Bean(BASE_MYSQL_DATA_TARGET)
    @ConfigurationProperties(prefix = "spring.datasource.target")
    public DataSource newDataSource2(){
        return new HikariDataSource();
    }

    @Bean(BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    public JdbcTemplate newJdbcTemplate2(@Qualifier(BASE_MYSQL_DATA_TARGET) DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    public TransactionTemplate transactionTemplate2(@Qualifier(BASE_MYSQL_DATA_TARGET)DataSource dataSource){
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
        return new TransactionTemplate(dataSourceTransactionManager);
    }


    ////////////////////////////////////////////////////////

    public static final String DATA_SYNC_MYSQL_DATA_TARGET = "DATA_SYNC_MYSQL_DATA_TARGET";
    public static final String DATA_SYNC_MYSQL_JDBC_TEMPLATE_TARGET = "DATA_SYNC_MYSQL_JDBC_TEMPLATE_TARGET";
    public static final String DATA_SYNC_MYSQL_JDBC_TRANSACTION_TARGET = "DATA_SYNC_MYSQL_JDBC_TRANSACTION_TARGET";

    @Bean(DATA_SYNC_MYSQL_DATA_TARGET)
    @ConfigurationProperties(prefix = "spring.datasource.datasync")
    public DataSource newDataSource3(){
        return new HikariDataSource();
    }

    @Bean(DATA_SYNC_MYSQL_JDBC_TEMPLATE_TARGET)
    public JdbcTemplate newJdbcTemplate3(@Qualifier(DATA_SYNC_MYSQL_DATA_TARGET) DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(DATA_SYNC_MYSQL_JDBC_TRANSACTION_TARGET)
    public TransactionTemplate transactionTemplate3(@Qualifier(DATA_SYNC_MYSQL_DATA_TARGET)DataSource dataSource){
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
        return new TransactionTemplate(dataSourceTransactionManager);
    }


    ////////////////////////////////////////////////////////


    @Value("${base.thread.extract}")
    Integer extractThreadNum;

    @Value("${base.thread.analysis}")
    Integer analysisThreadNum;

    public static final String BASE_ENGINE_COMPONENT = "BASE_ENGINE_COMPONENT";
    //文件提取任务
    public static final String EXTRACT_JOBS = "EXTRACT_JOBS";
    //文件分析任务
    public static final String ANALYSIS_JOBS = "ANALYSIS_JOBS";

    @Bean(BASE_ENGINE_COMPONENT)
    public EngineComponent<Object> engineComponent(){
        HashMap<String,PipeLineComponent<Object>> temp = new HashMap<String, PipeLineComponent<Object>>(){};
        temp.put(EXTRACT_JOBS,new PipeLineComponent<Object>(EXTRACT_JOBS,extractThreadNum){});
        temp.put(ANALYSIS_JOBS,new PipeLineComponent<Object>(ANALYSIS_JOBS,analysisThreadNum){});
        return new EngineComponent<Object>(temp){};
    }
}
