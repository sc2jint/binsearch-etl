package com.binsearch.engine.entity.db;

import javax.persistence.*;

@Entity
@Table(name = "view_detail_tasks")
public class ViewDetailTasks {

    @Id
    @Column(name = "id")
    public Integer id;

    @Basic
    @Column(name = "task_id")
    public String taskId;

    @Basic
    @Column(name = "version_name")
    public String versionName;

    public String language;
}
