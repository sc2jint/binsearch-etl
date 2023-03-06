package com.binsearch.engine.entity.el;

import com.binsearch.engine.entity.db.FileFeature;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Document(indexName = "#{T(com.binsearch.engine.entity.el.index.FileFeatureIndexES).getSuffix()}")
public class FileFeatureES {

    @Id
    private String id;

    @Field(type = FieldType.Long)
    private Long componentFileId;

    @Field(type = FieldType.Text)
    private String componentId;

    @Field(type = FieldType.Integer,index = false)
    private Integer lineNumber;

    @Field(type = FieldType.Integer,index = false)
    private Integer tokenNumber;

    @Field(type = FieldType.Text)
    private String type0;

    @Field(type = FieldType.Text)
    private String type1;

    @Field(type = FieldType.Text)
    private String type2blind;

    @Field(type = FieldType.Text)
    private String sourceTable;


    public FileFeatureES(FileFeature fileFeature){
        setId(String.format("%s%d",fileFeature.getComponentId(),fileFeature.getId()));

    }

}
