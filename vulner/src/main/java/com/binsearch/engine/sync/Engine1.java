package com.binsearch.engine.sync;

import com.binsearch.engine.BaseEngineConfiguration;
import com.binsearch.engine.ETLService;
import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.etl.EngineComponent;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class Engine1 implements ETLService {
    Logger error_log_ = LoggerFactory.getLogger("ERROR_LOG_");

    Logger logger = LoggerFactory.getLogger(Engine.class);

    @Autowired
    @Qualifier(EngineConfiguration.SYNC_EXTRACT_JOB_SERVICE)
    ExtractJobService extractJobService;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_ENGINE_COMPONENT)
    EngineComponent<Object> engineComponent;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate targetJdbctemplat;

    @Autowired
    @Qualifier(BaseEngineConfiguration.BASE_MYSQL_JDBC_TRANSACTION_TARGET)
    TransactionTemplate targetTransactionTemplate;


    @Autowired
    @Qualifier(BaseEngineConfiguration.DATA_SYNC_MYSQL_JDBC_TEMPLATE_TARGET)
    JdbcTemplate syncJdbcTemplate;



    @Value("${dataSync.script.save.path}")
    String savePath;

    @Value("${dataSync.script.save.fileNum}")
    Integer fileNum;


    @Value("${dataSync.table.name}")
    String tableName;

    @Value("${dataSync.file.start.page}")
    Integer startPage;

    public void init()throws Exception{
        //初始化表

    }

    //增量处理
    public void incrementalComponent() {}

    //
    public void specifyComponent() {}

    //分页读取文件

    public List<String> readLine(File file, Integer s, Integer e) throws IOException {
        try (InputStream in = FileUtils.openInputStream(file)) {
            final InputStreamReader input = new InputStreamReader(in, Charsets.toCharset(Charset.defaultCharset()));
            final BufferedReader reader = IOUtils.toBufferedReader(input);
            final List<String> list = new ArrayList<>();

            //逃过指定行数
            if (s > 0) {
                while(true){
                    if(s == 0){
                        break;
                    }
                    reader.readLine();
                    s--;
                }
            }

            int readerNum = 0;
            String line = reader.readLine();
            while (line != null) {
                readerNum++;
                list.add(line);
                if(readerNum == e){
                    break;
                }
                line = reader.readLine();
            }

            return list;
        }
    }


    public void fullComponent() {
        for(int i = 1;i <= fileNum;i++){
            readFile(tableName,String.format("%s%s%s_%d.sql",savePath,File.separator,tableName,i),startPage);
        }
    }


    public void loadData2Cache(List list,String table,Integer startPage,String path) {
        try{
            WorkJob workJob = new WorkJob();
            workJob.setEntitys(list);
            workJob.setClazz(ComponentFile.class);
            workJob.setTableName(table);
            workJob.setStartPage(startPage);
            workJob.setPath(path);
            engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).addPipeLineJob(workJob);
            engineComponent.setCurWorkJobCount(1);

            while(engineComponent.getPipeLineComponent(BaseEngineConfiguration.EXTRACT_JOBS).threadNumDecrement()){
                extractJobService.fileExtract();
            }
        }catch (Exception e){
            error_log_.info(String.format("loadData2CacheError table=%s; page = %s; path = %s ;%s",table,String.valueOf(startPage),path,e.getMessage()));
        }
    }

    /**
     * 运行默认任务
     * */
    public void readFile(String table,String path,Integer startPage) {
        File file = new File(path);
        if(!file.exists()){
            error_log_.info(String.format("loadFileError table=%s; path=%s;文件不存在",table,path));
            return;
        }

        try (InputStream in = FileUtils.openInputStream(file)) {
            final InputStreamReader input = new InputStreamReader(in, Charsets.toCharset(Charset.defaultCharset()));
            final BufferedReader reader = IOUtils.toBufferedReader(input);
            List<Object> list = new ArrayList<>();
            int sipk = startPage * 20;

            //逃过指定行数
            if (sipk > 0) {
                while(true){
                    if(sipk == 0){
                        break;
                    }
                    reader.readLine();
                    sipk--;
                }
            }

            int readerNum = 0;
            String line = reader.readLine();
            while (line != null) {
                if(engineComponent.getCurWorkJobCount()>1000){
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    }catch (Exception e){}
                    continue;
                }

                readerNum++;
                list.add(line);
                if(readerNum == 20){
                    startPage++;
                    loadData2Cache(list,table,startPage,path);
                    readerNum = 0;
                    list = new ArrayList<>();
                }
                line = reader.readLine();
            }
            if(!CollectionUtils.isEmpty(list)){
                loadData2Cache(list,table,startPage,path);
            }
        }catch (Exception e){
            error_log_.info(String.format("loadFileError table=%s; path=%s;%s",table,path,e.getMessage()));
        }
        unResources(engineComponent);
    }
}