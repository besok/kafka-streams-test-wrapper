package ru.gpb.als.streams.test.helpers;

import org.apache.avro.specific.SpecificRecord;

/**
 * Functional interface for new objects generating.
 * @param <V> type for generating object.
 * Created by Boris Zhguchev on 12/09/2018
 */
public interface ValueProducer<V> {
  V produce();

  @SuppressWarnings("unchecked")
  default Class<V> getValClass() {
    V v = produce();
    if (v == null)
      return (Class<V>) SpecificRecord.class;
    else
      return (Class<V>) v.getClass();
  }

  ValueProducer<SpecificRecord> NULL_PRODUCER = ()->null;
}
