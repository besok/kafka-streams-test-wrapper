package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

/**
 * Field generator for null value @see {@link AvroGeneratorByType}
 * Created by Boris Zhguchev on 24/09/2018
 */
public class NullGenerator implements AvroGeneratorByType<Object>{
  @Override
  public Object generate(Schema schema) {
	return null;
  }
}
