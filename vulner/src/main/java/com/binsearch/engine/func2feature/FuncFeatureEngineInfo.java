package com.binsearch.engine.func2feature;

import javax.persistence.*;

/**
 * @author ylm
 * @description TODO
 * @date 2022-09-20
 */
@Entity
@Table(name = "func_feature_engine_info")
public class FuncFeatureEngineInfo {

    @Id
    @Column(name = "id")
    public Long id;

    @Basic
    @Column(name = "cur_file_table")
    public String curFileTable;

    @Basic
    @Column(name = "language")
    public String language;

    public String languageTableKeyValue;
}
