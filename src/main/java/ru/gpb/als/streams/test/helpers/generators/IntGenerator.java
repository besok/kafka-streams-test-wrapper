package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class IntGenerator implements AvroGeneratorByType<Integer> {

  private Random random = new Random();

  @Override
  public Integer generate(Schema schema) {
	return random.nextInt();
  }
}
