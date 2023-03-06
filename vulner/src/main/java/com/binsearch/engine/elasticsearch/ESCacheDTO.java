package com.binsearch.engine.elasticsearch;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ESCacheDTO {


    public static final Integer DATE_BASE_FLAG_SAVE_ = 1;

    public static final Integer DATE_BASE_FLAG_UN_SAVE_ = 0;

    Object cacheData;

    Integer cacheFlag;



}
