package com.binsearch.engine.file2feature;

import com.binsearch.engine.ETLService;
import com.binsearch.engine.file2feature.service.AnalysisJobService;
import com.binsearch.engine.file2feature.service.DataBaseJobService;
import com.binsearch.engine.file2feature.service.ExtractJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.file2feature.EngineConfiguration.FILE2FEATURE_CONFIGURATION;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 *
 */
@Configuration(value = FILE2FEATURE_CONFIGURATION)
public class EngineConfiguration {

    public static final String FILE2FEATURE_CONFIGURATION = "FILE2FEATURE_CONFIGURATION";

    ///////////////////////////////////////////////////////////

    public static final String FILE2FEATURE_ENGINE = "FILE2FEATURE_ENGINE";

    @Bean(FILE2FEATURE_ENGINE)
    public ETLService engine() {
        return new Engine();
    }


    ///////////////////////////////////////////////////////////


    public static final String FILE2FEATURE_DATABASE_JOB_SERVICE = "FILE2FEATURE_DATABASE_JOB_SERVICE";
    public static final String FILE2FEATURE_FEATUREEXTRACT_JOB_SERVICE = "FILE2FEATURE_FEATUREEXTRACT_JOB_SERVICE";
    public static final String FILE2FEATURE_RECORD_SERVICE = "FILE2FEATURE_RECORD_SERVICE";

    @Bean(FILE2FEATURE_DATABASE_JOB_SERVICE)
    public DataBaseJobService dataBaseJobService() {
        return new DataBaseJobService();
    }

    @Bean(FILE2FEATURE_FEATUREEXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService() {
        return new ExtractJobService();
    }

    @Bean(FILE2FEATURE_RECORD_SERVICE)
    public AnalysisJobService analysisJobService() {
        return new AnalysisJobService();
    }

}
