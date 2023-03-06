package com.binsearch.etl;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;


@Configuration
public class ETLConfiguration {
    public static final String ETL_DATA_SOURCE = "ELT_DATA_SOURCE";
    public static final String ETL_JDBC_TEMPLATE = "ELT_JDBC_TEMPLATE";
    public static final String ETL_JDBC_TRANSACTION_TEMPLATE = "ETL_JDBC_TRANSACTION_TEMPLATE";
    public static final String ETL_BASE_THREAD_POOL = "ETL_BASE_THREAD_POOL";


    private static final int CORE_POOL_SIZE = 100;
    private static final int MAX_POOL_SIZE = 100;
    private static final int QUEUE_CAPACITY = 50000;



    @Bean(ETL_BASE_THREAD_POOL)
    public Executor simpleAsync() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(CORE_POOL_SIZE);
        taskExecutor.setMaxPoolSize(MAX_POOL_SIZE);
        taskExecutor.setQueueCapacity(QUEUE_CAPACITY);
        taskExecutor.setKeepAliveSeconds(180);
        taskExecutor.setThreadNamePrefix("simpleAsync-");
        taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean(ETL_DATA_SOURCE)
    @ConfigurationProperties(prefix = "spring.datasource.etl")
    public DataSource dataSourceOne(){
        return new HikariDataSource();
    }

    @Bean(ETL_JDBC_TEMPLATE)
    public JdbcTemplate jdbcTemplate(@Qualifier(ETL_DATA_SOURCE)DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(ETL_JDBC_TRANSACTION_TEMPLATE)
    public DataSourceTransactionManager transactionTemplate(@Qualifier(ETL_DATA_SOURCE)DataSource dataSource){
        return new DataSourceTransactionManager(dataSource);
    }


}
