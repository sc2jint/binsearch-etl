package com.binsearch.engine.file2feature;

import lombok.Data;

import javax.persistence.*;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
@Data
public class FileFeatureEngineInfo {
    public String curFileTable;

    public String language;

    public String languageTableKeyValue;
}
