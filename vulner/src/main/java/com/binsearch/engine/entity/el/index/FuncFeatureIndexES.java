package com.binsearch.engine.entity.el.index;

public class FuncFeatureIndexES {
    private static String suffix;

    public static void setSuffix(String suffix){
        FuncFeatureIndexES.suffix = suffix;
    }


    public static String getSuffix(){
        return FuncFeatureIndexES.suffix;
    }
}
