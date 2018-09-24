package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class BooleanGenerator implements AvroGeneratorByType<Boolean> {
  private Random random = new Random();

  @Override
  public Boolean generate(Schema schema) {
	return random.nextInt(1) == 0;
  }
}
