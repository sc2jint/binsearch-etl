package com.binsearch.engine.entity.db;

import javax.persistence.*;

@Entity
@Table(name = "view_tasks")
public class ViewTasks {

    @Id
    @Column(name = "id")
    public Integer id;

    @Basic
    @Column(name = "task_id")
    public String taskId;

    @Basic
    @Column(name = "main_lang")
    public String mainLang;
}
