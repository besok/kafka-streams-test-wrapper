package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Field generator for Float @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class FloatGenerator implements AvroGeneratorByType<Float> {
  private Random random = new Random();

  @Override
  public Float generate(Schema schema) {
	return random.nextFloat();
  }
}
