package com.binsearch.engine.file2type3feature;

import com.binsearch.engine.ETLService;
import com.binsearch.engine.file2type3feature.service.AnalysisJobService;
import com.binsearch.engine.file2type3feature.service.DataBaseJobService;
import com.binsearch.engine.file2type3feature.service.ExtractJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.binsearch.engine.file2type3feature.EngineConfiguration.*;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 *
 */
@Configuration(value = EngineConfiguration.FILE2TYPE3FEATURE_CONFIGURATION)
public class EngineConfiguration {

    public static final String FILE2TYPE3FEATURE_CONFIGURATION = "FILE2TYPE3FEATURE_CONFIGURATION";

    ///////////////////////////////////////////////////////////

    public static final String FILE2TYPE3FEATURE_ENGINE = "FILE2TYPE3FEATURE_ENGINE";

    @Bean(FILE2TYPE3FEATURE_ENGINE)
    public ETLService engine() {
        return new Engine();
    }


    ///////////////////////////////////////////////////////////


    public static final String FILE2TYPE3FEATURE_DATABASE_JOB_SERVICE = "FILE2TYPE3FEATURE_DATABASE_JOB_SERVICE";
    public static final String FILE2TYPE3FEATURE_FEATUREEXTRACT_JOB_SERVICE = "FILE2TYPE3FEATURE_FEATUREEXTRACT_JOB_SERVICE";
    public static final String FILE2TYPE3FEATURE_RECORD_SERVICE = "FILE2TYPE3FEATURE_RECORD_SERVICE";

    @Bean(FILE2TYPE3FEATURE_DATABASE_JOB_SERVICE)
    public DataBaseJobService dataBaseJobService() {
        return new DataBaseJobService();
    }

    @Bean(FILE2TYPE3FEATURE_FEATUREEXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService() {
        return new ExtractJobService();
    }

    @Bean(FILE2TYPE3FEATURE_RECORD_SERVICE)
    public AnalysisJobService analysisJobService() {
        return new AnalysisJobService();
    }

}
