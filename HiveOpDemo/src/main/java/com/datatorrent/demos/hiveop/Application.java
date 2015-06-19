package com.datatorrent.demos.hiveop;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hive.HiveOperator;
import com.datatorrent.contrib.hive.HiveStore;
import com.datatorrent.demos.hiveop.SalesEventGenerator;
import com.datatorrent.demos.hiveop.JsonToSalesEventConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="HiveOp")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SalesEventGenerator input = dag.addOperator("SalesEventGenerator", new SalesEventGenerator());
    
    JsonToSalesEventConverter jtos = dag.addOperator("JSONToSalesDataConverter", new JsonToSalesEventConverter());

    FSRollingImpl fsRoll = dag.addOperator("FSRolling", new FSRollingImpl());
    
    HiveOperator hiveOp = dag.addOperator("HiveOp", new HiveOperator());
    HiveStore store = new HiveStore();
    hiveOp.setHivestore(store);
    
    dag.addStream("input_jtos", input.jsonBytes, jtos.input);
    dag.addStream("jtos_fsRoll", jtos.outputMap, fsRoll.input);
    dag.addStream("fsRoll_hiveOp", fsRoll.outputPort, hiveOp.input);
  }

}
