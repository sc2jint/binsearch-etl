package com.binsearch.engine.entity.el.index;

public class FileFeatureIndexES {
    private static String suffix;

    public static void setSuffix(String suffix){
        FileFeatureIndexES.suffix = suffix;
    }


    public static String getSuffix(){
        return FileFeatureIndexES.suffix;
    }
}
