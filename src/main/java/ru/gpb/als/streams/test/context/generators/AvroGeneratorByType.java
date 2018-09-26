package ru.gpb.als.streams.test.context.generators;

import org.apache.avro.Schema;

/**
 * Avro generator by field
 * @param <V> generate type
 *
 * Created by Boris Zhguchev on 24/09/2018
 */
interface AvroGeneratorByType<V> {


  /**
   * generate value from schema
   * @param schema @see {@link Schema}
   * @return generated value
   * */
  V generate(Schema schema);

  default V generate(Schema.Field field) {
	return generate(field.schema());
  }

}
