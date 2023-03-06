package com.binsearch.engine.component2file;

import com.binsearch.engine.ETLService;
import com.binsearch.engine.component2file.service.AnalysisJobService;
import com.binsearch.engine.component2file.service.DataBaseJobService;
import com.binsearch.engine.component2file.service.ExtractJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.component2file.EngineConfiguration.*;

@Configuration(value = COMPONENT2FILE_CONFIGURATION)
public class EngineConfiguration {

    public static final String COMPONENT2FILE_CONFIGURATION = "COMPONENT2FILE_CONFIGURATION";

    ////////////////////////////////////////////////////////

    public static final String COMPONENT2FILE_ENGINE = "COMPONENT2FILE_ENGINE";

    @Bean(COMPONENT2FILE_ENGINE)
    public ETLService engine(){
        return new Engine();
    }

    ////////////////////////////////////////////////////////

    public static final String COMPONENT2FILE_DATABASE_JOB_SERVICE = "COMPONENT2FILE_DATABASE_JOB_SERVICE";
    public static final String COMPONENT2FILE_EXTRACT_JOB_SERVICE = "COMPONENT2FILE_EXTRACT_JOB_SERVICE";
    public static final String COMPONENT2FILE_ANALYSIS_JOB_SERVICE = "COMPONENT2FILE_ANALYSIS_JOB_SERVICE";

    @Bean(COMPONENT2FILE_DATABASE_JOB_SERVICE)
    public DataBaseJobService dataBaseJobService(){
        return new DataBaseJobService();
    }

    @Bean(COMPONENT2FILE_EXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService(){
        return new ExtractJobService();
    }

    @Bean(COMPONENT2FILE_ANALYSIS_JOB_SERVICE)
    public AnalysisJobService analysisJobService(){
        return new AnalysisJobService();
    }



    ////////////////////////////////////////////////////////


}
