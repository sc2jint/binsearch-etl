package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.Constant;
import com.binsearch.engine.component2file.ComponentFileDTO;
import com.binsearch.engine.entity.db.ComponentCacheInfo;
import com.binsearch.engine.file2feature.FileFeatureDTO;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Task {

    Cache<String,ESCacheDTO> loadingCache;

    ComponentCacheInfo componentCacheInfo;

    Map<Constant.ModelType,Object> cfgMap;

    public void initLoadingCache(long cacheMaximum,RemovalListener<String,ESCacheDTO> removalListener){
        if(Objects.isNull(this.loadingCache)) {
            this.loadingCache = Caffeine.newBuilder()
                    .initialCapacity(100)
                    .maximumSize(cacheMaximum)
                    .removalListener(removalListener)
                    .build();
        }
    }
}
