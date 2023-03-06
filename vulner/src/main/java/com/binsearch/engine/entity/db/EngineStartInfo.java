package com.binsearch.engine.entity.db;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "engine_start_info")
public class EngineStartInfo {

    @Id
    @Column(name = "id")
    public Long id;

    @Column(name = "engine_type")
    public String engineType;

    @Column(name = "running_info")
    public String runningInfo;
}
