package com.datatorrent.demos.hiveop;

import java.util.ArrayList;

import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator;
import com.google.common.collect.Lists;

public class FSRollingImpl extends AbstractFSRollingOutputOperator<SalesEvent>
{
  private static final long serialVersionUID = 13246461761L;

  @Override
  public ArrayList<String> getHivePartition(SalesEvent tuple)
  {
    ArrayList<String> hivePartitions = new ArrayList<String>();
    return hivePartitions;
  }

  @Override
  protected byte[] getBytesForTuple(SalesEvent tuple)
  {
    String s = tuple.timestamp + "," + 
               tuple.productId + "," + 
               tuple.customerId + "," + 
               tuple.channelId + "," + 
               tuple.regionId + "," + 
               tuple.amount + "," +
               tuple.discount + "," +
               tuple.tax + "\n";
    
    return s.getBytes();
  }

}
