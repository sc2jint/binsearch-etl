package com.binsearch.engine.entity.db;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "t_component_file")
public class ComponentFile {
    @Id
    @Column(name = "id")
    public Long id;

    @Basic
    @Column(name = "component_id")
    public String componentId;

    @Basic
    @Column(name = "source_file_path")
    public String sourceFilePath;


    @Basic
    @Column(name = "file_path")
    public String filePath;

    @Basic
    @Column(name = "language")
    public String language;

    @Basic
    @Column(name = "component_version_id")
    public String componentVersionId;


    @Basic
    @Column(name = "file_hash_value")
    public String fileHashValue;
}
