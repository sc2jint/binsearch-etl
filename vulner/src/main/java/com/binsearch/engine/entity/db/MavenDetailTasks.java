package com.binsearch.engine.entity.db;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "maven_detail_task")
public class MavenDetailTasks {

    @Id
    @Column(name = "id")
    public Integer id;

    @Basic
    @Column(name = "compo_id")
    public String compoId;


    @Basic
    @Column(name = "pom_status_code")
    public String pomStatusCode;

    @Basic
    @Column(name = "source_code_addr")
    public String sourceCodeAddr;

    @Basic
    @Column(name = "current_version")
    public String currentVersion;


}
