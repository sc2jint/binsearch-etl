package com.binsearch.engine.entity.db;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "maven_task")
public class MavenTasks {
    @Id
    @Column(name = "id")
    public Integer id;

    @Basic
    @Column(name = "compo_id")
    public String compoId;


}
