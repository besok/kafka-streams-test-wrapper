package ru.gpb.als.streams.test.mock;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 *
 * Default serde. It is needed for injection @see {@link MockLightSchemaRegistryClient}
 * Created by Boris Zhguchev on 03/09/2018
 */
public class MockKafkaAvroSerde<T extends SpecificRecord> implements Serde<T> {
  public MockKafkaAvroSerde() {
    serializer = new SerializeWrapper<>();
    deserializer=new DeserializeWrapper<>();
  }

  private Serializer<T> serializer;
  private Deserializer<T> deserializer;


  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }

  @Override
  public void configure(Map<String, ?> serdeConfig, boolean isSerdeForRecordKeys) {

    this.serializer().configure(serdeConfig, isSerdeForRecordKeys);
    this.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
  }

  @Override
  public void close() {
    this.serializer().close();
    this.deserializer().close();
  }

  private class SerializeWrapper<T extends SpecificRecord> implements Serializer<T> {

    private final MockKafkaAvroSerializer inner;

    private SerializeWrapper() {
      inner = new MockKafkaAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      this.inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
      return this.inner.serialize(topic, data);
    }

    @Override
    public void close() {
      this.inner.close();
    }
  }
  private class DeserializeWrapper<T extends SpecificRecord> implements Deserializer<T> {

    private final MockKafkaAvroDeserializer inner;

    private DeserializeWrapper() {
      this.inner = new MockKafkaAvroDeserializer();
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      this.inner.configure(configs, isKey);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
      return (T) this.inner.deserialize(topic, data);
    }

    @Override
    public void close() {
      this.inner.close();
    }
  }

}
