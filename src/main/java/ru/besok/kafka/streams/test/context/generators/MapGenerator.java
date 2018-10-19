package ru.besok.kafka.streams.test.context.generators;

import org.apache.avro.Schema;

import java.util.HashMap;

/**
 * Field generator for Map @see {@link AvroGeneratorByType}
 * Created by Boris Zhguchev on 24/09/2018
 */
public class MapGenerator<K,V> implements AvroGeneratorByType<HashMap<K,V>> {

  // NO-OP , in order to not to write impl - don't need now.
  @Override
  public HashMap<K, V> generate(Schema schema) { return new HashMap<>(); }
}
