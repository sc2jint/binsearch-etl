package com.binsearch.engine.sync;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.etl.ETLConfiguration;
import com.binsearch.etl.EngineComponent;
import com.binsearch.etl.PipeLineComponent;
import com.binsearch.etl.orm.JdbcUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ExtractJobService {

    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(BaseEngineConfiguration.DATA_SYNC_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate jdbcTemplate;



    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void fileExtract(){
        PipeLineComponent<Object> pipeLineComponent =
                engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS);

        try {
            while (engineComponent.isJobRun()) {
                Object job = pipeLineComponent.getPipeLineJobs();
                if (Objects.isNull(job)) {
                    if (engineComponent.getCurWorkJobCount() == 0) {
                        break;
                    } else {
                        try {
                            TimeUnit.MILLISECONDS.sleep(200);
                        } catch (Exception e) {}
                        continue;
                    }
                }
                analyzeExtract((WorkJob) job);
            }
        }catch (Exception e){}
        finally {
            pipeLineComponent.threadNumIncrement();
        }
    }

    @Async(ETLConfiguration.ETL_BASE_THREAD_POOL)
    public void analyzeExtract(WorkJob job){
        try{
            List<Object> temp = new ArrayList<>();
            try{
                if(job.getClazz() == ComponentFile.class){
                    job.getEntitys().stream().forEach(t -> {
                        String text = t.toString();
                        ComponentFile componentFile = new ComponentFile();
                        text = text.substring(text.indexOf("values(")+7).replace(");","");
                        String[] values = text.split(",");

                        componentFile.setId(Long.valueOf(values[0]));
                        componentFile.setComponentId(Strings.isBlank(values[1])?"":values[1].substring(1, values[1].length()-1));
                        componentFile.setSourceFilePath(Strings.isBlank(values[2])?"":values[2].substring(1, values[2].length()-1));
                        componentFile.setFilePath(Strings.isBlank(values[3])?"":values[3].substring(1, values[3].length()-1));
                        componentFile.setLanguage(Strings.isBlank(values[4])?"":values[4].substring(1, values[4].length()-1));
                        componentFile.setComponentVersionId(Strings.isBlank(values[5])?"":values[5].substring(1, values[5].length()-1));
                        componentFile.setFileHashValue(Strings.isBlank(values[6])?"":values[6].substring(1, values[6].length()-1));
                        temp.add(componentFile);
                    });
                }
            }catch (Exception e){
                error_log_.info(String.format("batchTranscodingError table = %s;page = %s;path = %s;%s",job.getTableName(),job.getStartPage(),job.getPath(),e.getMessage()));
            }

            try {
                if(!CollectionUtils.isEmpty(temp)) {
                    new JdbcUtils(jdbcTemplate).table(job.getTableName()).batchCreate(temp.toArray());
                }
            }catch (Exception e){
                error_log_.info(String.format("batchError table = %s;page = %s;path = %s;%s",job.getTableName(),job.getStartPage(),job.getPath(),e.getMessage()));
            }
        }finally{
            engineComponent.curWorkJobCountDecrement();
        }
    }
}
