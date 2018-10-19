package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Field generator for Boolean @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class BooleanGenerator implements AvroGeneratorByType<Boolean> {
  private Random random = new Random();

  @Override
  public Boolean generate(Schema schema) {
	return random.nextInt(1) == 0;
  }
}
