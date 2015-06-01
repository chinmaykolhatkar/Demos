package com.datatorrent.demos.naivebayes.mahout.trainer;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.demos.naivebayes.mahout.utils.NaiveBayesUtils;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class WeightsTableOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(WeightsTableOperator.class);

  private boolean hasChanged;
  private LinkedHashMap<String, VectorWritable> table = new LinkedHashMap();

  private String modelDir;

  public final transient DefaultOutputPort<KeyValPair<String, VectorWritable>> output = new DefaultOutputPort<KeyValPair<String, VectorWritable>>();

  public final transient DefaultInputPort<KeyValPair<String, VectorWritable>> input = new DefaultInputPort<KeyValPair<String, VectorWritable>>()
  {
    @Override public void process(KeyValPair<String, VectorWritable> tuple)
    {
      String label = tuple.getKey();
      Vector instance = tuple.getValue().get();
      if (!table.containsKey(label))
      {
        table.put(label, new VectorWritable(instance));
      }
      else
      {
        Vector v = table.get(label).get();
        v.assign(instance, Functions.PLUS);
      }
      hasChanged = true;
    }
  };

  @Override public void beginWindow(long windowId)
  {
    this.hasChanged = false;
  }

  @Override public void endWindow()
  {
    if (this.hasChanged) {
      try
      {
        NaiveBayesUtils.writeLabelIndexFile(new Configuration(), new Path(this.modelDir), this.table.keySet());

        Iterator it = this.table.entrySet().iterator();
        while (it.hasNext())
        {
          Map.Entry pair = (Map.Entry)it.next();
          String label = (String)pair.getKey();
          VectorWritable row = (VectorWritable)pair.getValue();
          this.output.emit(new KeyValPair(label, row));
        }
      }
      catch (IOException e)
      {
        logger.error("Failed to write label index file to disk", e);
      }
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
