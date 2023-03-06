package com.binsearch.engine;

import com.binsearch.engine.entity.db.ComponentFile;
import com.binsearch.engine.entity.db.FileFeature;
import com.binsearch.engine.entity.db.FuncFeature;

public class Constant {

    public enum LanguageType {
        JAVA("JAVA","java",new String[]{"java"}),
        C("C","c",new String[]{"c"}),
        CPP("C++","cpp",new String[]{"cpp","hpp"}),
        PYTHON("PYTHON","python",new String[]{"py"}),
        GOLANG("GOLANG","golang",new String[]{"go"}),
        JAVASCRIPT("JAVASCRIPT","javascript",new String[]{"js"}),
        PHP("PHP","php",new String[]{"php"}),
        RUBY("RUBY","ruby",new String[]{"rb"}),
        RUST("RUST","rust",new String[]{"rs"}),
        CS("C#","cs",new String[]{"cs"});

        //开发语言
        private String languageName;

        //别名
        private String alias;

        //文件后缀名
        private String[] expandNames;

        LanguageType(String languageName,String alias,String[] expandNames){
            this.expandNames = expandNames;
            this.alias = alias;
            this.languageName = languageName;
        }

        public String getAlias(){
            return this.alias;
        }

        public String getLanguageName(){
            return this.languageName;
        }

        public String[] getExpandNames(){
            return this.expandNames;
        }

        public static LanguageType getLanguageTypeForAlias(String alias){
            LanguageType temp = null;
            for(LanguageType model:LanguageType.values()){
                if(model.getAlias().equals(alias)){
                    temp = model;
                    break;
                }
            }
            return temp;
        }

        public static LanguageType getLanguageTypeForLanguageName(String languageName){
            LanguageType temp = null;
            for(LanguageType model:LanguageType.values()){
                if(model.getLanguageName().equals(languageName)){
                    temp = model;
                    break;
                }
            }
            return temp;
        }

    }

    public enum ModelType{
        MODEL_COMPONENT_FILE(
                "componentFile",
                "t_component_file",
                "componentFileName",
                "component_file_name",
                 ComponentFile.class
        ),

        MODEL_FILE_FEATURE(
                "fileFeature",
                "t_sourcecode_%s_filefeature",
                "sourcecode%sFilefeatureName",
                "sourcecode_%s_filefeature_name",
                FileFeature.class),

        MODEL_FUNC_FEATURE(
                "funcFeature",
                "t_sourcecode_%s_funcfeature",
                "sourcecode%sFuncfeatureName",
                "sourcecode_%s_funcfeature_name",
                FuncFeature.class);




        //类型名
        private String modelName;

        //对应的表名模式
        private String tableNameModel;

        //对应缓存表属性名
        private String componentCacheAttrName;

        //对于缓存表列名
        private String componentCacheTableColumnName;

        //业务实体类
        private Class entityClass;



        public Class getEntityClass() {
            return this.entityClass;
        }

        public String getModelName(){
            return this.modelName;
        }

        public String getTableNameModel(){
            return this.tableNameModel;
        }

        public String getComponentCacheAttrName(){
            return this.componentCacheAttrName;
        }

        public String getComponentCacheTableColumnName(){
            return this.componentCacheTableColumnName;
        }

        ModelType(String modelName,String tableNameModel,String componentCacheAttrName,String componentCacheTableColumnName,Class entityClass){
            this.modelName = modelName;
            this.tableNameModel = tableNameModel;
            this.componentCacheAttrName = componentCacheAttrName;
            this.componentCacheTableColumnName = componentCacheTableColumnName;
            this.entityClass = entityClass;
        }

        public static ModelType getModelType(String modelName){
            ModelType temp = null;
            for(ModelType model:ModelType.values()){
                if(model.getModelName().equals(modelName)){
                    temp = model;
                    break;
                }
            }
            return temp;
        }

    }
}
