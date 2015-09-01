package com.datatorrent.demos.enrichment;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * @displayName Enrichment POJO To JSON Converter
 * @category Converter
 * @tags enrichment, json, converter, pojo
 * @since 3.1.0
 */
public class EnrichmentPOJOToJSON extends BaseOperator
{
  private transient ObjectWriter writer;
  private Class<?> inputClass;
  
  public DefaultOutputPort<String> out = new DefaultOutputPort<String>();
  
  @InputPortFieldAnnotation(schemaRequired=true)
  public DefaultInputPort<Object> in = new DefaultInputPort<Object>() {
    @Override
    public void setup(PortContext context)
    {
      inputClass = context.getValue(PortContext.TUPLE_CLASS);
      ObjectMapper mapper = new ObjectMapper();
      writer = mapper.writerWithType(inputClass);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, true);
      mapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, true);
    }

    @Override
    public void process(Object tuple)
    {
      try {
        out.emit(writer.writeValueAsString(tuple));
      } catch (IOException e) {
        e.printStackTrace();
        out.emit(e.toString());
      }
    }
  };
}
