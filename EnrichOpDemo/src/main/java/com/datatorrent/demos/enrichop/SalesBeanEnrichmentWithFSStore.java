package com.datatorrent.demos.enrichop;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.BeanEnrichmentOperator;
import com.datatorrent.contrib.enrichment.FSLoader;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by chinmay on 29/5/15.
 */
@ApplicationAnnotation(name="SalesBeanEnrichmentWithFSStore")
public class SalesBeanEnrichmentWithFSStore implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration configuration)
  {
    SalesEventGenerator input = dag.addOperator("SalesEventGenerator", new SalesEventGenerator());

    JsonToSalesEventConverter jtos = dag.addOperator("JSONToSalesDataConverter", new JsonToSalesEventConverter());

    BeanEnrichmentOperator bean = dag.addOperator("BeanEnrichment", new BeanEnrichmentOperator());
    FSLoader fsstore = new FSLoader();
    fsstore.setFileName(configuration.get("dt.application.SalesBeanEnrichmentWithFSStore.operator.store.fileName"));
    bean.setStore(fsstore);

    ConsoleOutputOperator out = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("input_jtos", input.jsonBytes, jtos.input);
    dag.addStream("jtos_bean", jtos.outputMap, bean.input);
    dag.addStream("bean_console", bean.output, out.input);
  }
}
