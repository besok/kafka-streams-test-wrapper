package ru.gpb.als.streams.test.helpers;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * KeyValueStoreHandler @see {@link KeyValueStore}
 * in case using aggregation functions @see {@link org.apache.kafka.streams.kstream.KTable} this store has been created.
 *
 * @param <K> key type for record @see {@link KeyValueStore}
 * @param <V> value type for record @see {@link KeyValueStore}
 *
 *
 *
 * Created by Boris Zhguchev on 12/09/2018
 */
public class KeyValueStoreHandler<K extends SpecificRecord, V extends SpecificRecord> {
  private String topic;
  private Class<K> clzzKey;
  private Class<V> clzzVal;
  private KeyValueStore<K, V> store;
  private Set<String> possibleStores;
  private StreamsTestHelperContext ctx;

  public KeyValueStoreHandler(String topic, Class<K> clzzKey, Class<V> clzzVal,StreamsTestHelperContext ctx) {
    this.topic = topic;
    this.clzzKey = clzzKey;
    this.clzzVal = clzzVal;
    this.ctx=ctx;
    findStores(topic);
  }

  private void findStores(String topic) {
    possibleStores = new HashSet<>();
    findSub(topic).ifPresent(sub -> {
      for (TopologyDescription.Node node : sub.nodes()) {
        if (node instanceof InternalTopologyBuilder.Processor) {
          possibleStores.addAll(((InternalTopologyBuilder.Processor) node).stores());
        }
      }
    });
  }

  /**
   * Trying to find pair with key.
   * @param key key
   * @return Optional
   *
   * */
  // TODO: 9/11/2018 Если у нас несколько сторов в стриме с 1 типом KV
  public Optional<V> find(K key) {
    for (String possibleStore : possibleStores) {
      KeyValueStore<K, V> kvStore = ctx.driver.getKeyValueStore(possibleStore);
      return Optional.of(kvStore.get(key));
    }
    return Optional.empty();
  }

  private Optional<TopologyDescription.Subtopology> findSub(String topic) {
    TopologyDescription topDsc = ctx.toppology();

    for (TopologyDescription.Subtopology subtop : topDsc.subtopologies()) {
      for (TopologyDescription.Node node : subtop.nodes()) {
        String name = node.name();
        if (node instanceof InternalTopologyBuilder.Sink) {
          if (((InternalTopologyBuilder.Sink) node).topic().equals(topic)) {
            return Optional.of(subtop);
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   *
   * method for returning to context and call other entities.
   * */
  public StreamsTestHelperContext pipe(){
    return this.ctx;
  }
}
