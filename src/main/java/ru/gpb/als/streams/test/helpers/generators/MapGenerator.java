package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;

import java.util.HashMap;

/**
 * Field generator for Map @see {@link AvroGeneratorByType}
 * Created by Boris Zhguchev on 24/09/2018
 */
public class MapGenerator<K,V> implements AvroGeneratorByType<HashMap<K,V>> {

  // now we don't need do this implemenation
  @Override
  public HashMap<K, V> generate(Schema schema) {
	return new HashMap<>();
  }
}
