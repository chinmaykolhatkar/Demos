package com.datatorrent.demos.naivebayes.mahout.trainer;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.demos.naivebayes.mahout.utils.NaiveBayesUtils;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class NBTrainOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(NBTrainOutputOperator.class);

  private transient Vector weightsPerFeature;
  private transient Vector weightsPerLabel;
  private transient Vector perLabelThetaNormalizer;
  private transient Matrix matrix;
  private transient HashMap<String, Integer> labelMap;

  private String modelDir;

  public transient final DefaultInputPort<KeyValPair<String, VectorWritable>> input = new DefaultInputPort<KeyValPair<String, VectorWritable>>()
  {
    @Override public void process(KeyValPair<String, VectorWritable> tuple)
    {
      logger.info("Processing tuple...");
      String labelString = (String)tuple.getKey();
      Vector instance = ((VectorWritable)tuple.getValue()).get();
      try
      {
        if (weightsPerFeature == null) {
          weightsPerFeature = new RandomAccessSparseVector(instance.size(), instance.getNumNondefaultElements());
        }
        weightsPerFeature.assign(instance, Functions.PLUS);
        if (weightsPerLabel == null)
        {
          labelMap = NaiveBayesUtils.readLabelIndexFile(new Configuration(), new Path(modelDir));
          weightsPerLabel = new DenseVector(labelMap.size());
        }
        if (!labelMap.containsKey(labelString)) {
          return;
        }
        int label = labelMap.get(labelString).intValue();
        weightsPerLabel.set(label, weightsPerLabel.get(label) + instance.zSum());
        if (matrix == null) {
          matrix = new SparseMatrix(weightsPerLabel.size(), weightsPerFeature.size());
        }
        matrix.assignRow(label, instance);
      }
      catch (IOException e)
      {
        logger.error("Failed to create matrix required for Naive Bayes model.", e);
      }
    }
  };

  @Override public void beginWindow(long windowId)
  {
    this.perLabelThetaNormalizer = new DenseVector();
  }

  @Override public void endWindow()
  {
    if ((this.weightsPerFeature != null) && (this.matrix != null))
    {
      try
      {
        Configuration conf = new Configuration();
        Path path = new Path(this.modelDir);
        NaiveBayesUtils.writeNaiveBayesModel(conf, path, this.matrix, this.weightsPerFeature, this.weightsPerLabel, this.perLabelThetaNormalizer, 1.0F);
        logger.info("NaiveBayes Model written to disk");
      }
      catch (IOException e)
      {
        logger.error("Failed to write Naive Bayes model to disk.", e);
      }
    }
    this.weightsPerFeature = null;
    this.matrix = null;
    this.weightsPerLabel = null;
    this.perLabelThetaNormalizer = null;
    this.labelMap = null;
  }

  public String getModelDir()
  {
    return modelDir;
  }

  public void setModelDir(String modelDir)
  {
    this.modelDir = modelDir;
  }
}
