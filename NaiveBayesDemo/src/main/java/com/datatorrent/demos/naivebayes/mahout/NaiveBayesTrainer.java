package com.datatorrent.demos.naivebayes.mahout;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.naivebayes.mahout.trainer.NBTrainInputOperator;
import com.datatorrent.demos.naivebayes.mahout.trainer.NBTrainOutputOperator;
import com.datatorrent.demos.naivebayes.mahout.trainer.WeightsTableOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="NaiveBayesTrainer")
public class NaiveBayesTrainer implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration configuration)
  {
    NBTrainInputOperator input = dag.addOperator("vectorize", new NBTrainInputOperator());
    WeightsTableOperator intermediate = dag.addOperator("weights", new WeightsTableOperator());
    NBTrainOutputOperator output = dag.addOperator("model", new NBTrainOutputOperator());

    dag.addStream("vectorize_weights", input.data, intermediate.input);
    dag.addStream("weights_model", intermediate.output, output.input);
  }
}
