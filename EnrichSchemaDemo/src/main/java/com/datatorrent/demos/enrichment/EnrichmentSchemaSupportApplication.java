package com.datatorrent.demos.enrichment;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.POJOSchemaEnrichmentOperator;

@ApplicationAnnotation(name = "EnrichmentSchemaSupportDemo")
public class EnrichmentSchemaSupportApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EnrichmentInputDataGenerator inpJSONGenerator = dag.addOperator("InputJSON", EnrichmentInputDataGenerator.class);
    EnrichmentInputParser parser = dag.addOperator("JSONParser", EnrichmentInputParser.class);
    POJOSchemaEnrichmentOperator op = dag.addOperator("Bean", POJOSchemaEnrichmentOperator.class);
    EnrichmentPOJOToJSON out = dag.addOperator("POJOToJSON", EnrichmentPOJOToJSON.class);
  }

}
