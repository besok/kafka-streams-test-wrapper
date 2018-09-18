package ru.gpb.als.streams.test.tests;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import ru.gpb.als.model.collectors.CountryCustomerCollector;
import ru.gpb.als.model.envelopes.AskCustomerEnvelope;
import ru.gpb.als.streams.test.helpers.generators.AvroGenerator;

import java.util.Random;

import static java.lang.String.join;
import static java.lang.String.valueOf;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class BaseAvroTest {

  @Test
  public void test(){

    AvroGenerator<CountryCustomerCollector> g = new AvroGenerator<>(CountryCustomerCollector.class);
    CountryCustomerCollector generate = g.generate();
    System.out.println(generate);
  }





}
