package com.binsearch.engine.sync;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkJob {

   private List<Object> entitys;

   private String tableName;

   private Class clazz;

   private Integer startPage;

   private String path;

}
