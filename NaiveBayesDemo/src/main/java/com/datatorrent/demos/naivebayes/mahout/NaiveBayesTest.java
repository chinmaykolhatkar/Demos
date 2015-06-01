package com.datatorrent.demos.naivebayes.mahout;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.demos.naivebayes.mahout.test.ClassifierOperator;
import com.datatorrent.demos.naivebayes.mahout.test.NBTestInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="NaiveBayesTest")
public class NaiveBayesTest implements StreamingApplication
{
  private DAG.Locality locality = null;

  @Override public void populateDAG(DAG dag, Configuration configuration)
  {
    NBTestInputOperator inp = (NBTestInputOperator)dag.addOperator("vectorize", new NBTestInputOperator());
    ClassifierOperator classifier = (ClassifierOperator)dag.addOperator("classifier", new ClassifierOperator());
    ConsoleOutputOperator out = (ConsoleOutputOperator)dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("vectorize_classifier", inp.data, classifier.input).setLocality(this.locality);
    dag.addStream("classifier_console", classifier.output, out.input).setLocality(this.locality);
  }
}
