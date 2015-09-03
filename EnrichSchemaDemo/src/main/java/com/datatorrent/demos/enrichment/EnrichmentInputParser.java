package com.datatorrent.demos.enrichment;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * @displayName Enrichment JSON Parser
 * @category Parser
 * @tags enrichment, json, parser
 * @since 3.1.0
 */
public class EnrichmentInputParser extends BaseOperator
{
  private transient ObjectReader reader;
  private Class<?> outputClass;

  public transient DefaultInputPort<String> in = new DefaultInputPort<String>() {
    @Override
    public void process(String tuple)
    {
      try {
        out.emit(reader.readValue(tuple));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  };
  
  @OutputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultOutputPort<Object> out = new DefaultOutputPort<Object>() {
    @Override
    public void setup(PortContext context)
    {
      outputClass = context.getValue(PortContext.TUPLE_CLASS);
      ObjectMapper mapper = new ObjectMapper();
      reader = mapper.reader(outputClass);
    }
  };
}
