package com.binsearch.engine.output;

import com.binsearch.engine.entity.db.ComponentCacheInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class WorkJob {

    ComponentCacheInfo cacheInfo;

    String tableNames;

    String currentTableName;

    String currentTableNameValue;



}
