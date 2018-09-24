package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class NullGenerator implements AvroGeneratorByType<Object>{
  @Override
  public Object generate(Schema schema) {
	return null;
  }
}
