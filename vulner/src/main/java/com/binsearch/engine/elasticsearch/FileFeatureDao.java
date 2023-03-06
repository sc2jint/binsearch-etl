package com.binsearch.engine.elasticsearch;

import com.binsearch.engine.entity.el.FileFeatureES;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface FileFeatureDao  extends ElasticsearchRepository<FileFeatureES,String>
{
    Optional<FileFeatureES> findById(String id);

    List<FileFeatureES> findByComponentId(String componentId);
}
