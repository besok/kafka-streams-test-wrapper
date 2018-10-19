package ru.besok.kafka.streams.test.context.generators;

/**
 * Interface for updating returned field
 *
 * @param <V> field type
 *            <p>
 *            Created by Boris Zhguchev on 24/09/2018
 */
public interface FieldUpdater<V> {

  /**
   * Simplify development style for plain cases.
   */
  static <V> FieldUpdater<V> constant(V val) {
	return oldVal -> val;
  }

  /**
   * @param oldVal value getting from generators
   * @return newVal generated this method. It can be a new value or modified the old value.
   */
  V update(V oldVal);
}

