package com.datatorrent.demos.enrichment.pojo;

public class Enriched
{
  int id;
  String personName;
  int personAge;

  public Enriched()
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

  public String getPersonName()
  {
    return personName;
  }

  public void setPersonName(String personName)
  {
    this.personName = personName;
  }

  public int getPersonAge()
  {
    return personAge;
  }

  public void setPersonAge(int personAge)
  {
    this.personAge = personAge;
  }

  @Override
  public String toString()
  {
    return "Enriched [id=" + id + ", personName=" + personName + ", personAge=" + personAge + "]";
  }
}
