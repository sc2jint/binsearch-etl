package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.Constant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.invoke.StringConcatFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WorkJob {


    //
    private Task task;

    //当前导出数据表
    private String currentTable;

    //当前数据模型
    private Constant.ModelType currentModelType;

    private Constant.LanguageType currentLanguageType;

    private AtomicInteger count = new AtomicInteger(0);




}
