package com.datatorrent.demos.hiveop;

public class SalesEvent
{
  /* dimension keys */
  public long timestamp;
  public int productId;
  public int customerId;
  public int channelId;
  public int regionId;

  /* metrics */
  public double amount;
  public double discount;
  public double tax;

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public int getProductId()
  {
    return productId;
  }

  public void setProductId(int productId)
  {
    this.productId = productId;
  }

  public int getCustomerId()
  {
    return customerId;
  }

  public void setCustomerId(int customerId)
  {
    this.customerId = customerId;
  }

  public int getChannelId()
  {
    return channelId;
  }

  public void setChannelId(int channelId)
  {
    this.channelId = channelId;
  }

  public int getRegionId()
  {
    return regionId;
  }

  public void setRegionId(int regionId)
  {
    this.regionId = regionId;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public double getDiscount()
  {
    return discount;
  }

  public void setDiscount(double discount)
  {
    this.discount = discount;
  }

  public double getTax()
  {
    return tax;
  }

  public void setTax(double tax)
  {
    this.tax = tax;
  }
}
