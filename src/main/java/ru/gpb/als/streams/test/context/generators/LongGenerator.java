package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Field generator for Long @see {@link AvroGeneratorByType}
 * Created by Boris Zhguchev on 24/09/2018
 */
public class LongGenerator implements AvroGeneratorByType<Long> {
  private Random random = new Random();
  @Override
  public Long generate(Schema schema) {
	return random.nextLong();
  }
}
