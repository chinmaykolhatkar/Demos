package com.datatorrent.demos.naivebayes.mahout.test;

import au.com.bytecode.opencsv.CSVReader;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.naivebayes.mahout.utils.NaiveBayesUtils;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class NBTestInputOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(NBTestInputOperator.class);

  public final transient DefaultOutputPort<KeyValPair<Integer, VectorWritable>> data = new DefaultOutputPort<KeyValPair<Integer, VectorWritable>>();
  private transient CSVReader arffReader;
  private String testFilePath;
  private String modelDir;
  private HashMap<String, Integer> labels;

  @Override public void emitTuples()
  {
    try {
      String[] line;
      while ((line = this.arffReader.readNext()) != null)
      {
        Vector vector = new RandomAccessSparseVector(line.length - 1, line.length - 1);
        for (int rowIndex = 0; rowIndex < line.length - 1; rowIndex++) {
          vector.set(rowIndex, Double.parseDouble(line[rowIndex]));
        }
        String label = line[(line.length - 1)];

        this.data.emit(new KeyValPair(this.labels.get(label), new VectorWritable(vector)));
      }
    } catch (IOException e) {
      logger.error("Failed to emit given tuple.", e);
    }
  }

  @Override public void beginWindow(long l)
  {
    try {
      this.labels = NaiveBayesUtils.readLabelIndexFile(new Configuration(), new Path(this.modelDir));
    } catch (IOException e) {
      logger.error("Failed to read label index file.", e);
    }
  }

  @Override public void endWindow()
  {
  }

  @Override public void setup(Context.OperatorContext context)
  {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      this.arffReader = new CSVReader(new InputStreamReader(fs.open(new Path(this.testFilePath))));
    } catch (IOException e) {
      logger.error("Failed to initialize test input file.", e);
    }
  }

  @Override public void teardown()
  {
    try {
      this.arffReader.close();
    } catch (IOException e) {
      logger.error("Failed to close test input file.", e);
    }
  }

  public String getTestFilePath()
  {
    return testFilePath;
  }

  public void setTestFilePath(String testFilePath)
  {
    this.testFilePath = testFilePath;
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
