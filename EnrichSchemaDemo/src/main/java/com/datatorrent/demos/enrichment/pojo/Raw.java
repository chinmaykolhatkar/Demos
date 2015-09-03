package com.datatorrent.demos.enrichment.pojo;

public class Raw
{
  int id;
  String name;
  int jobCode;
  
  public Raw()
  {
  }
  
  public int getId()
  {
    return id;
  }
  public void setId(int id)
  {
    this.id = id;
  }
  public String getName()
  {
    return name;
  }
  public void setName(String name)
  {
    this.name = name;
  }
  public int getJobCode()
  {
    return jobCode;
  }
  public void setJobCode(int jobCode)
  {
    this.jobCode = jobCode;
  }
}
