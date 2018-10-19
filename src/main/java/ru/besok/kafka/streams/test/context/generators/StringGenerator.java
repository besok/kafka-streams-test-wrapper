package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.Random;

import static java.lang.String.join;
import static java.lang.String.valueOf;

/**
 * Field generator for String @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class StringGenerator implements AvroGeneratorByType<String> {
  private Random random = new Random();
  @Override
  public String generate(Schema schema) {
	int rand = random.nextInt(1000);
	char[] chars = new char[50];
	for (int i = 0; i < chars.length; i++) {
	  chars[i] = (char) (random.nextInt(26) + 'a');
	}

	return join("-", valueOf(rand), valueOf(chars));
  }
}
