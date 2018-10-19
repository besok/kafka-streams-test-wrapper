package ru.besok.kafka.streams.test.data;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Created by Boris Zhguchev on 05/09/2018
 */

@Configuration
public class StreamsCfg {


  @Bean
  @Qualifier(value = "streams")
  public Properties streamProperties(){
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9091");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put("schema.registry.url", "http://localhost:8081");
    props.put("key.converter.schema.registry.url", "http://localhost:8081");
    props.put("value.converter.schema.registry.url", "http://localhost:8081");
    return props;
  }
  @Bean
  public StreamsBuilder builder(){
   return new StreamsBuilder();
  }

}
