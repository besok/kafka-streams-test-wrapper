package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.Random;

/**
 * Field generator for byte[] @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class BytesGenerator implements AvroGeneratorByType<byte[]> {
  private Random random = new Random();

  @Override
  public byte[] generate(Schema schema) {
	byte[] bytes = new byte[1000];
	random.nextBytes(bytes);
	return bytes;
  }
}
