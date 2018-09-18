package ru.gpb.als.streams.test.helpers;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import ru.gpb.als.streams.test.configuration.TestKafkaAvroSerde;

import java.util.*;

/**
 * Context for underlying operations.
 * <p>
 * Created by Boris Zhguchev on 11/09/2018
 */

// TODO: 9/12/2018 Добавить историю...
// TODO: 9/12/2018 Добавить сценарии валидации (посылаем, принимаем и т.п.)
// TODO: 9/11/2018 Парсинг топологии
@SuppressWarnings("all")
public class StreamsTestHelperContext {
  private StreamsBuilder streamsBuilder;
  private Map<String, Object> streamsPropertiesMap;
  protected TopologyTestDriver driver;

  private ConsumerRecordGenerator latestSender;
  private ProducerRecordHandler latestReceiver;
  private KeyValueStoreHandler latestKeeper;


  public StreamsTestHelperContext(StreamsBuilder streamsBuilder, Properties streamsProperties) {
    this.streamsBuilder = streamsBuilder;
    this.streamsPropertiesMap = fromStreamProp(streamsProperties);
    this.driver = new TopologyTestDriver(streamsBuilder.build(), streamsProperties);
  }

  protected <T extends SpecificRecord> TestKafkaAvroSerde<T> serde(Class<T> clzz, boolean isKey) {
    TestKafkaAvroSerde<T> serde = new TestKafkaAvroSerde<>();
    serde.configure(streamsPropertiesMap, isKey);
    return serde;
  }

  protected TopologyDescription toppology() {
    return streamsBuilder.build().describe();
  }

  private Map<String, Object> fromStreamProp(Properties streamsProperties) {
    Map<String, Object> map = new HashMap<>();
    for (final String name : streamsProperties.stringPropertyNames())
      map.put(name, streamsProperties.getProperty(name));
    return map;
  }

  /**
   * generate new sender @see {@link ConsumerRecordGenerator}
   *
   * @param topic         topic name
   * @param keyProducer   producer for generating key
   * @param valueProducer producer for generating value
   * @return ConsumerRecordGenerator
   */
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> ConsumerRecordGenerator<K, V> sender(
      String topic,
      ValueProducer<K> keyProducer,
      ValueProducer<V> valueProducer
  ) {
    this.latestSender = new ConsumerRecordGenerator<>(topic, keyProducer, valueProducer, this);
    return this.latestSender;
  }

  /**
   * generate new sender @see {@link ConsumerRecordGenerator} with @see {@link AvroValueProducer}
   *
   * @param topic         topic name
   * @param keyProducer   producer for generating key
   * @param valueProducer producer for generating value
   * @return ConsumerRecordGenerator
   */
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> CompositeConsumerRecordGenerator<K, V> sender(
      String topic,
      Class<K> keyClass,
      Class<V> valueClass
  ) {

    return new CompositeConsumerRecordGenerator<>(
        topic,
        new AvroValueProducer<>(keyClass),
        new AvroValueProducer<>(valueClass),
        this);
  }

  /**
   * generate new receiver @see {@link ProducerRecordHandler}
   *
   * @param topic   topic name
   * @param keyType key type
   * @param valType val type
   * @return ProducerRecordHandler
   */
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> ProducerRecordHandler<K, V> receiver(String topic, Class<K> keyType, Class<V> valType) {
    this.latestReceiver = new ProducerRecordHandler<>(topic, keyType, valType, this);
    return this.latestReceiver;
  }

  /**
   * generate new receiver @see {@link ProducerRecordHandler}
   * This receiver will be generated  by information getting from prev sender.
   *
   * @return ProducerRecordHandler
   * @throws IllegalStateException
   */
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> ProducerRecordHandler<K, V> receiver() {
    checkForGenerating(this.latestSender);
    this.latestReceiver =
        new ProducerRecordHandler<>(
            latestSender.getTopic(),
            latestSender.getKeyProducer().getValClass(),
            latestSender.getValProducer().getValClass(),
            this);
    return this.latestReceiver;
  }

  /**
   * generate new keeper @see {@link KeyValueStoreHandler}
   *
   * @param topic   topic name
   * @param keyType key type
   * @param valType val type
   * @return KeyValueStoreHandler
   **/
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> KeyValueStoreHandler<K, V> keeper(String topic, Class<K> keyType, Class<V> valType) {
    this.latestKeeper = new KeyValueStoreHandler<>(topic, keyType, valType, this);
    return this.latestKeeper;
  }

  /**
   * generate new keeper @see {@link KeyValueStoreHandler} .
   * this keeper will be generated  by information getting from prev sender.
   *
   * @return KeyValueStoreHandler
   * @throws IllegalStateException
   **/
  public <K extends SpecificRecordBase, V extends SpecificRecordBase> KeyValueStoreHandler<K, V> keeper() {
    checkForGenerating(this.latestSender);
    Class<K> keyClass = latestSender.getKeyProducer().getValClass();
    Class<V> valClass = latestSender.getValProducer().getValClass();
    this.latestKeeper = new KeyValueStoreHandler<>(latestSender.getTopic(), keyClass, valClass, this);
    return this.latestKeeper;
  }


  private void checkForGenerating(Object o) {
    if (Objects.isNull(o))
      throw new IllegalStateException("impossible generate new entity because there isn't previous state operation, for example [.sender(...)]");
  }

  public void close() {
    driver.close();
  }

}
