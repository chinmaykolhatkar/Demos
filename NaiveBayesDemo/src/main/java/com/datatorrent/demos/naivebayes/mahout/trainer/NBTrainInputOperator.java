package com.datatorrent.demos.naivebayes.mahout.trainer;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class NBTrainInputOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(NBTrainInputOperator.class);
  private String trainFilePath;

  private transient BufferedReader arffReader;

  public final transient DefaultOutputPort<KeyValPair<String, VectorWritable>> data = new DefaultOutputPort<KeyValPair<String, VectorWritable>>();

  @Override public void emitTuples()
  {
    String line;
    try {
      while ((line = this.arffReader.readLine()) != null) {
        if ((line.isEmpty() || line.startsWith("%") || line.startsWith("@"))) {
          return;
        }

        String[] params = line.split(",");
        Vector vector = new RandomAccessSparseVector(params.length -1, params.length - 1);
        for (int rowIndex = 0; rowIndex < params.length - 1; rowIndex++) {
          vector.set(rowIndex, Double.parseDouble(params[rowIndex]));
        }
        String label = params[(params.length - 1)];

        this.data.emit(new KeyValPair(label, new VectorWritable(vector)));      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override public void beginWindow(long l)
  {
  }

  @Override public void endWindow()
  {
  }

  @Override public void setup(Context.OperatorContext context)
  {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      this.arffReader = new BufferedReader(new InputStreamReader(fs.open(new Path(this.trainFilePath))));
    } catch (IOException e) {
      throw new RuntimeException("Failed to open arff reader stream", e);
    }
  }

  @Override public void teardown()
  {
    try {
      this.arffReader.close();
    } catch (IOException e) {
      logger.error("Failed to close arff reader stream", e);
    }
  }

  public String getTrainFilePath()
  {
    return trainFilePath;
  }

  public void setTrainFilePath(String trainFilePath)
  {
    this.trainFilePath = trainFilePath;
  }
}
