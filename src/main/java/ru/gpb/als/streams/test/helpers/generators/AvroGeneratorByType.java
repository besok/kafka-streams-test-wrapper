package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

/**
 * Created by Boris Zhguchev on 24/09/2018
 */
interface AvroGeneratorByType<V> {
  V generate(Schema schema);

  default V generate(Schema.Field field) {
	return generate(field.schema());
  }

}
