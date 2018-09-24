package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.List;
import java.util.Random;

/**
 *
 * Field generator for Enums @see {@link AvroGeneratorByType}
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
public class EnumGenerator<E extends Enum<E>> implements AvroGeneratorByType<Enum<E>> {

  private Random random = new Random();

  @Override
  public Enum<E> generate(Schema schema) {
	try {
	  List<String> enumList = schema.getEnumSymbols();
	  @SuppressWarnings("unchecked")
	  Class<E> aClass = (Class<E>) Class.forName(schema.getFullName());
	  return Enum.valueOf(aClass, enumList.get(random.nextInt(enumList.size())));
	} catch (ClassNotFoundException e) {
	  e.printStackTrace();
	}
	return null;
  }
}
