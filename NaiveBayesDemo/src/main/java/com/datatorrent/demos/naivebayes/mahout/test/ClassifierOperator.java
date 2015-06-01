package com.datatorrent.demos.naivebayes.mahout.test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.AbstractVectorClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClassifierOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(ClassifierOperator.class);

  private String modelDir;

  private long lastModelTimestamp = -1;
  private transient AbstractVectorClassifier classifier;

  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  public transient final DefaultInputPort<KeyValPair<Integer, VectorWritable>> input = new DefaultInputPort<KeyValPair<Integer, VectorWritable>>()
  {
    @Override public void process(KeyValPair<Integer, VectorWritable> tuple)
    {
      Vector prediction = classifier.classifyFull(tuple.getValue().get());
      double bestScore = -Double.MAX_VALUE;
      int predictedLabelId = -1;
      for (Vector.Element el : prediction.all())
      {
        int id = el.index();
        double score = el.get();
        if (score > bestScore)
        {
          bestScore = score;
          predictedLabelId = id;
        }
      }
      if (predictedLabelId == tuple.getKey().intValue()) {
        output.emit("true");
      } else {
        output.emit("false");
      }
    }
  };

  @Override public void beginWindow(long windowId)
  {
    try
    {
      Configuration conf = new Configuration();
      Path path = new Path(this.modelDir);
      FileSystem fs = FileSystem.get(conf);
      long ts = fs.getFileStatus(new Path(path, "naiveBayesModel.bin")).getModificationTime();
      if (ts > this.lastModelTimestamp) {
        logger.debug("File has changed. Reloading the model...");
        NaiveBayesModel model = NaiveBayesModel.materialize(path, new Configuration());
        this.classifier = new StandardNaiveBayesClassifier(model);
      } else {
        logger.debug("File has not changed.");
      }
    } catch (IOException e) {
      logger.error("Failed to load latest Naive Bayes Model", e);
    }
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
