package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Field generator for Double @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class DoubleGenerator implements AvroGeneratorByType<Double> {

  private Random random = new Random();

  @Override
  public Double generate(Schema schema) {
	return random.nextDouble();
  }
}
