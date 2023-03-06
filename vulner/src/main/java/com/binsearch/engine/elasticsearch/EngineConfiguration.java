package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.ETLService;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import static com.binsearch.engine.elasticsearch.EngineConfiguration.ELASTICSEARCH_CONFIGURATION;

@Configuration(value = ELASTICSEARCH_CONFIGURATION)
public class EngineConfiguration {

    @Value("${elasticsearch.server.host}")
    String host;

    @Value("${elasticsearch.server.port}")
    Integer port;

    public static final String ELASTICSEARCH_CONFIGURATION = "ELASTICSEARCH_CONFIGURATION";

    public static final String ELASTICSEARCH_ENGINE = "ELASTICSEARCH_ENGINE";

    @Bean(ELASTICSEARCH_ENGINE)
    public ETLService engine(){
        return new Engine();
    }


    /////////////////////////////////////////////////////

    public static final String ELASTICSEARCH_EXTRACT_JOB_SERVICE = "ELASTICSEARCH_EXTRACT_JOB_SERVICE";


    @Bean(ELASTICSEARCH_EXTRACT_JOB_SERVICE)
    public ExtractJobService extractJobService(){
        return new ExtractJobService();
    }


    ////////////////////////////////////////////////////////////////////////

    public static final String ELASTICSEARCH_CLIENT = "elasticsearchTemplate";

    public static final String ELASTICSEARCH_CFG = "ELASTICSEARCH_CFG";


    @Bean(ELASTICSEARCH_CFG)
    public AbstractElasticsearchConfiguration elasticsearchClient() {
        return new AbstractElasticsearchConfiguration(){
            @Override
            public RestHighLevelClient elasticsearchClient() {
                RestClientBuilder builder = RestClient.builder(new HttpHost(host,port));
                return new RestHighLevelClient(builder);
            }
        };
    }

    @Bean(ELASTICSEARCH_CLIENT)
    public ElasticsearchRestTemplate getElasticsearchRestTemplate(@Qualifier(ELASTICSEARCH_CFG) AbstractElasticsearchConfiguration cfg){
        return new ElasticsearchRestTemplate(cfg.elasticsearchClient(), cfg.elasticsearchConverter(), cfg.resultsMapper());
    }
}
