package ru.gpb.als.streams.test.mock;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.isNull;

/**
 *
 * Custom mock implementation for SchemaRegistryClient
 * @see SchemaRegistryClient
 *
 * It consists of in-memory cache(hashmap) and mocks all requests to server.
 *
 * Created by Boris Zhguchev on 03/09/2018
 */
public class MockInMemorySchemaRegistryClient
    implements SchemaRegistryClient {

  private static AtomicInteger sequencer = new AtomicInteger(0);
  private static ConcurrentMap<String, Value> cache = new ConcurrentHashMap<>();


  private int register(String subject, String schema) {
    Value value = cache.get(subject);
    if (isNull(value)) {
      value = new Value(schema,sequencer.incrementAndGet());
      cache.put(subject, value);
    }

    return value.id;
  }

  private String fetch(int id) {
    for (Value v : cache.values()) {
      if (v.id == id)
        return v.schema;
    }
    throw new IllegalArgumentException(" not found id = " + id);
  }

  @Override
  public int register(String s, Schema schema) throws IOException, RestClientException {
    return register(s, schema.toString());
  }

  @Override
  public Schema getByID(int i) throws IOException, RestClientException {
    return new Schema.Parser().parse(fetch(i));
  }

  @Override
  public Schema getById(int i) throws IOException, RestClientException {
    return new Schema.Parser().parse(fetch(i));
  }

  @Override
  public Schema getBySubjectAndID(String s, int i) throws IOException, RestClientException {
    return null;
  }

  @Override
  public Schema getBySubjectAndId(String s, int i) throws IOException, RestClientException {
    return null;
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(String s) throws IOException, RestClientException {
    return null;
  }

  @Override
  public SchemaMetadata getSchemaMetadata(String s, int i) throws IOException, RestClientException {
    return null;
  }

  @Override
  public int getVersion(String s, Schema schema) throws IOException, RestClientException {
    return 1;
  }

  @Override
  public List<Integer> getAllVersions(String s) throws IOException, RestClientException {
    return null;
  }

  @Override
  public boolean testCompatibility(String s, Schema schema) throws IOException, RestClientException {
    return false;
  }

  @Override
  public String updateCompatibility(String s, String s1) throws IOException, RestClientException {
    return null;
  }

  @Override
  public String getCompatibility(String s) throws IOException, RestClientException {
    return null;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return null;
  }

  @Override
  public int getId(String s, Schema schema) throws IOException, RestClientException {
    Value value = cache.get(s);
    if (isNull(value)) {
      value = new Value(schema.toString(),sequencer.incrementAndGet());
      cache.put(s, value);
    }

    return value.id;
  }

  @Override
  public List<Integer> deleteSubject(String s) throws IOException, RestClientException {
    return null;
  }

  @Override
  public List<Integer> deleteSubject(Map<String, String> map, String s) throws IOException, RestClientException {
    return null;
  }

  @Override
  public Integer deleteSchemaVersion(String s, String s1) throws IOException, RestClientException {
    return null;
  }

  @Override
  public Integer deleteSchemaVersion(Map<String, String> map, String s, String s1) throws IOException, RestClientException {
    return null;
  }


  private class Value {
    private String schema;
    private int id;

    Value(String schema, int id) {
      this.schema = schema;
      this.id = id;
    }

  }
}
