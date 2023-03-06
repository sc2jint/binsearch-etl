package com.binsearch.engine.entity.db;


import com.binsearch.etl.entity.DataEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "engine_running_error")
public class EngineRunningError{
    @Id
    @Column(name = "id")
    public Long id;

    @Column(name = "component_id")
    public String ComponentId;

    @Column(name = "error_date")
    public Date errorDate;

    @Column(name="target_table")
    public String targetTable;

    @Basic
    @Column(name = "running_error_info")
    public String runningErrorInfo;

    @Basic
    @Column(name="source_table")
    public String sourceTable;

    @Basic
    @Column(name="engine_type")
    public String engineType;

}
