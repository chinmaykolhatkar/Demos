package com.datatorrent.demos.naivebayes.mahout.utils;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class NaiveBayesUtils
{
  public static void writeLabelIndexFile(Configuration conf, Path labelIndexPath, Set<String> labelsMap)
      throws IOException
  {
    FileSystem fs = labelIndexPath.getFileSystem(conf);
    if (!fs.exists(labelIndexPath)) {
      fs.mkdirs(labelIndexPath);
    }
    OutputStreamWriter osw = new OutputStreamWriter(fs.create(new Path(labelIndexPath, "labelIndex.csv"), true));
    CSVWriter csvWriter = new CSVWriter(osw);
    Iterator it = labelsMap.iterator();
    int labelId = 0;
    while (it.hasNext())
    {
      String label = (String)it.next();
      csvWriter.writeNext(new String[] { label, Integer.toString(labelId++) });
    }
    csvWriter.close();
  }

  public static HashMap<String, Integer> readLabelIndexFile(Configuration conf, Path path)
      throws IOException
  {
    HashMap<String, Integer> labelsMap = new HashMap();

    FileSystem fs = path.getFileSystem(conf);
    InputStreamReader isr = new InputStreamReader(fs.open(new Path(path, "labelIndex.csv")));
    CSVReader reader = new CSVReader(isr);
    String[] line;
    while ((line = reader.readNext()) != null) {
      labelsMap.put(line[0], new Integer(line[1]));
    }
    return labelsMap;
  }

  public static void writeNaiveBayesModel(Configuration conf, Path path, Matrix matrix, Vector weightsPerFeature, Vector weightsPerLabel, Vector perLabelThetaNormalizer, float alphaI)
      throws IOException
  {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
    NaiveBayesModel model = new NaiveBayesModel(matrix, weightsPerFeature, weightsPerLabel, perLabelThetaNormalizer, alphaI);
    model.validate();
    model.serialize(path, conf);
  }
}
