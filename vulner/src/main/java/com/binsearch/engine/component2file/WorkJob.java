package com.binsearch.engine.component2file;


import com.binsearch.engine.entity.db.MavenDetailTasks;
import com.binsearch.engine.entity.db.ViewDetailTasks;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkJob {

   private Task task;

   private Object detailTask;

   private String sourceType;

   private String sourceFilePath;

   public String getTaskId(){
      return sourceType.equals(Engine.SOURCE_TYPE_GIT)?
              ((ViewDetailTasks)detailTask).taskId:((MavenDetailTasks)detailTask).compoId;
   }
   public String getVersionName(){
      return sourceType.equals(Engine.SOURCE_TYPE_GIT)?
              ((ViewDetailTasks)detailTask).versionName:((MavenDetailTasks)detailTask).currentVersion;
   }

   public void errorLog(String str){
      try {
         task.errorLog(getTaskId(), str);
      }catch (Exception e){}
   }

}
