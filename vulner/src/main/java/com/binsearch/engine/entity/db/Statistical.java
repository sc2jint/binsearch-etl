package com.binsearch.engine.entity.db;


import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Data
@NoArgsConstructor
@Entity
@Table(name = "statistical")
public class Statistical {
    @Column(name = "max_num")
    public Integer maxNum;

    @Column(name = "min_num")
    public Integer minNum;

    @Column(name = "count_num")
    public Integer countNum;

}
