package com.datatorrent.demos.enrichment;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.FSLoader;
import com.datatorrent.contrib.enrichment.TupleEnrichmentOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "EnrichShemaSupportStaticApp")
public class EnrichmentSchemaSupportApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EnrichmentInputDataGenerator inpJSONGenerator = dag.addOperator("InputJSON", EnrichmentInputDataGenerator.class);
    EnrichmentInputParser parser = dag.addOperator("JSONParser", EnrichmentInputParser.class);
    TupleEnrichmentOperator enrich = dag.addOperator("Enrich", TupleEnrichmentOperator.class);
    ConsoleOutputOperator out = dag.addOperator("Console", ConsoleOutputOperator.class);
    
    FSLoader fsstore = new FSLoader();
    enrich.setStore(fsstore);
    
    dag.addStream("input", inpJSONGenerator.out, parser.in);
    dag.addStream("parsedData", parser.out, enrich.inputPojo);
    dag.addStream("enrichedData", enrich.outputPojo, out.input);
  }

}
