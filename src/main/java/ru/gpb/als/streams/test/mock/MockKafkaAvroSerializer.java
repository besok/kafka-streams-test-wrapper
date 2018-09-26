package ru.gpb.als.streams.test.mock;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Default serializer. It is needed for injection @see {@link MockLightSchemaRegistryClient}
 * Created by Boris Zhguchev on 03/09/2018
 */
public class MockKafkaAvroSerializer extends KafkaAvroSerializer {
  public MockKafkaAvroSerializer() {
    super(new MockLightSchemaRegistryClient());
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
