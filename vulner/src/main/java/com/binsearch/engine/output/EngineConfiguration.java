package com.binsearch.engine.output;

import com.binsearch.engine.ETLService;

import com.binsearch.engine.component2file.service.AnalysisJobService;
import com.binsearch.engine.output.service.ExtractJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.output.EngineConfiguration.OUTPUTSQL_CONFIGURATION;

@Configuration(value = OUTPUTSQL_CONFIGURATION)
public class EngineConfiguration {


    public static final String OUTPUTSQL_CONFIGURATION = "OUTPUTSQL_CONFIGURATION";

    ////////////////////////////////////////////////////////

    public static final String OUTPUTSQL_ENGINE = "OUTPUTSQL_ENGINE";

    @Bean(OUTPUTSQL_ENGINE)
    public ETLService engine(){
        return new Engine();
    }

    ////////////////////////////////////////////////////////

    public static final String OUTPUTSQL_EXTRACT_JOB_SERVICE = "OUTPUTSQL_EXTRACT_JOB_SERVICE";


    @Bean(OUTPUTSQL_EXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService(){
        return new ExtractJobService();
    }




}
