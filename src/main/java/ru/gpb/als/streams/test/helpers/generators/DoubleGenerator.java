package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
public class DoubleGenerator implements AvroGeneratorByType<Double> {

  private Random random = new Random();

  @Override
  public Double generate(Schema schema) {
	return random.nextDouble();
  }
}
