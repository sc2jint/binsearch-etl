package com.binsearch.engine.entity.db;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "component_cache_")
public class ComponentCacheInfo {

    //运行状态-运行结束
    public static final Integer RUNNING_FLAGS_END = 1;

    //运行状态-运行中,未结束
    public static final Integer RUNNING_FLAGS_UN_END = 0;



    //组件类型-合格组件
    public static final String COMPONENT_TYPE_DEFAULT = "COMPONENT_TYPE_DEFAULT";

    //组件类型-用户临时组件
    public static final String COMPONENT_TYPE_TEMP = "COMPONENT_TYPE_TEMP";


    @Id
    @Column(name = "id")
    public Long id;

    @Basic
    @Column(name = "component_id")
    public String componentId;

    @Basic
    @Column(name = "component_file_name")
    public String componentFileName;

    @Basic
    @Column(name = "sourcecode_java_filefeature_name")
    public String sourcecodeJavaFilefeatureName;


    @Basic
    @Column(name = "sourcecode_java_filetype3feature_name")
    public String sourcecodeJavaFiletype3featureName;



    @Basic
    @Column(name = "sourcecode_java_funcfeature_name")
    public String sourcecodeJavaFuncfeatureName;


    @Basic
    @Column(name = "sourcecode_cpp_filefeature_name")
    public String sourcecodeCppFilefeatureName;

    @Basic
    @Column(name = "sourcecode_c_filefeature_name")
    public String sourcecodeCFilefeatureName;

    @Basic
    @Column(name = "sourcecode_python_filefeature_name")
    public String sourcecodePythonFilefeatureName;

    @Basic
    @Column(name = "sourcecode_golang_filefeature_name")
    public String sourcecodeGolangFilefeatureName;


    @Basic
    @Column(name = "sourcecode_c_funcfeature_name")
    public String sourcecodeCFuncfeatureName;

    @Basic
    @Column(name = "sourcecode_python_funcfeature_name")
    public String sourcecodePythonFuncfeatureName;

    @Basic
    @Column(name = "sourcecode_golang_funcfeature_name")
    public String sourcecodeGolangFuncfeatureName;

    @Basic
    @Column(name = "sourcecode_cpp_funcfeature_name")
    public String sourcecodeCppFuncfeatureName;

    @Basic
    @Column(name = "component_version")
    public String componentVersion;

    @Basic
    @Column(name = "component2file_flag")
    public Integer component2fileFlag;

    @Basic
    @Column(name = "file2feature_flag")
    public Integer file2featureFlag;

    @Basic
    @Column(name = "func2feature_flag")
    public Integer func2featureFlag;

    @Basic
    @Column(name = "file2type3feature_flag")
    public Integer file2type3featureFlag;

    @Basic
    @Column(name = "components_type")
    public String componentType;

    @Basic
    @Column(name = "main_language")
    public String mainLanguage;

    @Basic
    @Column(name = "code_source")
    public String codeSource;


    public Integer num;
}
