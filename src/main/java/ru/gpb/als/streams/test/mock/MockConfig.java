package ru.gpb.als.streams.test.mock;

import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

/**
 *
 * Default tests config.
 *
 * That properties consists of Test* classes.
 *
 * Created by Boris Zhguchev on 07/09/2018
 */
@TestConfiguration
public class MockConfig {
  @Bean
  @Qualifier(value = "streams")
  public Properties streamProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-2");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9091");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MockKafkaAvroSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MockKafkaAvroSerde.class);
    props.put("schema.registry.url", "http://localhost:8081");
    props.put("key.converter.schema.registry.url", "http://localhost:8081");
    props.put("value.converter.schema.registry.url", "http://localhost:8081");
    return props;
  }

}
