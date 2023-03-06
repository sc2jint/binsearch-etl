package com.binsearch.engine.entity.db;


import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import javax.persistence.*;
import java.sql.Timestamp;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */

@Data
@NoArgsConstructor
@Entity
@Table(name = "t_sourcecode_filefeature")
public class FileFeature {

    @Id
    @Column(name = "id")
    public Long id;

    @Basic
    @Column(name = "component_file_id")
    public Long componentFileId;


    @Basic
    @Column(name = "component_id")
    public String componentId;

    @Basic
    @Column(name = "line_number")
    public Integer lineNumber;

    @Basic
    @Column(name = "token_number")
    public Integer tokenNumber;

    @Basic
    @Column(name = "type0")
    public String type0;

    @Basic
    @Column(name = "type1")
    public String type1;

    @Basic
    @Column(name = "type2blind")
    public String type2blind;

    @Basic
    @Column(name = "source_table")
    public String sourceTable;

    @Basic
    @Column(name = "create_date")
    public Timestamp createDate;
}
