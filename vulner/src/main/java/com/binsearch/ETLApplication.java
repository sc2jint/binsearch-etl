package com.binsearch;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.hibernate.boot.jaxb.internal.stax.FilteringXMLEventReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@EnableAsync
@EnableScheduling
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class })
public class ETLApplication {

    public static void main(String[] args) throws Exception {
       SpringApplication.run(ETLApplication.class, args);
    }

}

