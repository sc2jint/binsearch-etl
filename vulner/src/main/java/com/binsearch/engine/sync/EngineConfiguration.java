package com.binsearch.engine.sync;

import com.binsearch.engine.ETLService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.sync.EngineConfiguration.SYNC_CONFIGURATION;

@Configuration(value = SYNC_CONFIGURATION)
public class EngineConfiguration {

    public static final String SYNC_CONFIGURATION = "SYNC_CONFIGURATION";

    ////////////////////////////////////////////////////////

    public static final String SYNC_ENGINE = "SYNC_ENGINE";

    @Bean(SYNC_ENGINE)
    public ETLService engine(){
        return new Engine1();
    }

    ////////////////////////////////////////////////////////
    public static final String SYNC_EXTRACT_JOB_SERVICE = "SYNC_EXTRACT_JOB_SERVICE";


    @Bean(SYNC_EXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService(){
        return new ExtractJobService();
    }

    ////////////////////////////////////////////////////////


}
