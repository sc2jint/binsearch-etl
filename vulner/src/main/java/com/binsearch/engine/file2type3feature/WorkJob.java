package com.binsearch.engine.file2type3feature;

import com.binsearch.engine.entity.db.ComponentFile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ylm
 * @description TODO
 * @date 2022-08-26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkJob {

    private Task task;

    private ComponentFile componentFile;

}
