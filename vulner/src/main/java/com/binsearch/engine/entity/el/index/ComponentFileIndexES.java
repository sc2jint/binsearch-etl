package com.binsearch.engine.entity.el.index;

public class ComponentFileIndexES {
    private static String suffix;

    public static void setSuffix(String suffix){
        ComponentFileIndexES.suffix = suffix;
    }


    public static String getSuffix(){
        return ComponentFileIndexES.suffix;
    }
}
