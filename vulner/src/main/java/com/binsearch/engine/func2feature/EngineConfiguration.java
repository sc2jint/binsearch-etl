package com.binsearch.engine.func2feature;

import com.binsearch.engine.ETLService;
import com.binsearch.engine.func2feature.service.DataBaseJobService;
import com.binsearch.engine.func2feature.service.ExtractJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.func2feature.EngineConfiguration.FUNC2FEATURE_CONFIGURATION;


/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */

@Configuration(value = FUNC2FEATURE_CONFIGURATION)
public class EngineConfiguration {

    public static final String FUNC2FEATURE_CONFIGURATION = "FUNC2FEATURE_CONFIGURATION";


    ///////////////////////////////////////////////////////////


    public static final String FUNC2FEATURE_ENGINE = "FUNC2FEATURE_ENGINE";

    @Bean(FUNC2FEATURE_ENGINE)
    public ETLService engine() {
        return new Engine();
    }


    ///////////////////////////////////////////////////////////



    public static final String FUNC2FEATURE_DATABASE_JOB_SERVICE = "FUNC2FEATURE_DATABASE_JOB_SERVICE";
    public static final String FUNC2FEATURE_FEATUREEXTRACT_JOB_SERVICE = "FUNC2FEATURE_FEATUREEXTRACT_JOB_SERVICE";

    @Bean(FUNC2FEATURE_DATABASE_JOB_SERVICE)
    public DataBaseJobService dataBaseJobService() {
        return new DataBaseJobService();
    }

    @Bean(FUNC2FEATURE_FEATUREEXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService() {
        return new ExtractJobService();
    }

}
