package ru.gpb.als.streams.test.configuration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Default deserializer. It is needed for injection @see {@link TestInMemorySchemaRegistryClient}
 * Created by Boris Zhguchev on 03/09/2018
 */
public class TestKafkaAvroDeserializer extends KafkaAvroDeserializer {
  public TestKafkaAvroDeserializer() {
    super(new TestInMemorySchemaRegistryClient());
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(withSpecific(configs), isKey);
  }

  public static Map<String, Object> withSpecific(Map<String, ?> config) {
    Map<String, Object> specificAvroEnabledConfig = config == null ? new HashMap<>() : new HashMap<>(config);
    specificAvroEnabledConfig.put("specific.avro.reader", true);
    return specificAvroEnabledConfig;
  }
}
