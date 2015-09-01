package com.datatorrent.demos.enrichment;

import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * @displayName Enrichment JSON Data Generator
 * @category Input
 * @tags enrichment, json
 * @since 3.1.0
 */
public class EnrichmentInputDataGenerator implements InputOperator
{
  private transient int maxEmitPerWindow = 10;
  private int emittedThisWindow = 0;
  
  Random random;
  private transient int MAX_JOB_CODE = 10;
  private transient String NAMES[] = {"ABC", "DEF", "LMN", "PQR", "XYZ"}; 
  
  public transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();
  
  
  @Override
  public void beginWindow(long windowId)
  {
    emittedThisWindow = 0;
  }

  private String generateJSON() throws JSONException
  {
    JSONObject jo = new JSONObject();
    int nextInt = random.nextInt(NAMES.length - 1);

    jo.put("id", nextInt+1);
    jo.put("name", NAMES[nextInt]);
    jo.put("jobCode", random.nextInt(MAX_JOB_CODE));
    
    return jo.toString();
  }

  @Override
  public void emitTuples()
  {
    try {
      if (emittedThisWindow < maxEmitPerWindow) {
        String json;
        json = generateJSON();
        out.emit(json);
        emittedThisWindow++;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    random = new Random();
  }

  @Override
  public void teardown()
  {
  }

  public int getMaxEmitPerWindow()
  {
    return maxEmitPerWindow;
  }

  public void setMaxEmitPerWindow(int maxEmitPerWindow)
  {
    this.maxEmitPerWindow = maxEmitPerWindow;
  }
}
