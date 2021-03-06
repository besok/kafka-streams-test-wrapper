package ru.besok.kafka.streams.test.integration;

import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import postgres.output.culimit.Envelope;
import postgres.output.culimit.Key;
import postgres.output.culimit.Value;
import ru.besok.kafka.streams.test.context.ValueProducer;
import ru.gpb.als.model.Country;
import ru.gpb.als.model.Customer;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.model.collectors.CUlimitCustomerCollector;
import ru.besok.kafka.streams.test.BaseStreamsTest;
import ru.besok.kafka.streams.test.StreamsTestHelper;
import ru.besok.kafka.streams.test.context.StreamsTestHelperContext;
import ru.besok.kafka.streams.test.context.generators.FieldUpdater;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.*;
import static ru.besok.kafka.streams.test.context.ValueProducer.*;
import static ru.besok.kafka.streams.test.data.StreamsUtils.H.*;
import static ru.besok.kafka.streams.test.context.generators.FieldUpdater.*;
import static ru.besok.kafka.streams.test.context.generators.FieldUpdaterPredicate.*;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class InitCommonTest extends BaseStreamsTest {

  @Autowired
  @Qualifier("streams")
  private Properties properties;
  @Autowired
  private StreamsBuilder builder;

  private int sequencer = 0;
  private String store = "internal.country_group_by_ask";
  private String stream = "internal.country";


  @Test
  public void shouldLastWinWithManuallyTest() {

	Optional<Country> countryOpt =
	  StreamsTestHelper.run(builder, properties)
		.sender(stream, NULL_PRODUCER, this::newCountryWithFixAskId)
		.send(5)
		.pipe()
		.keeper(store, Tuple1.class, Country.class)
		.find(defaultTuple1());
	--sequencer;

	assertTrue(countryOpt.isPresent());
	assertEquals(newCountryWithFixAskId(), countryOpt.get());

  }

  @Test
  public void shouldLastWinWithGeneratorTest() {
	StreamsTestHelperContext ctx = StreamsTestHelper.run(builder, properties);

	Country last =
	  ctx
		.sender(stream, Tuple1.class, Country.class)
		.rule(name("ask_id"), constant(10), false)
		.send(5)
		.last().value();

	Optional<Country> countryOpt =
	  ctx
		.keeper(store, Tuple1.class, Country.class)
		.find(wrap(last.getAskId()));

	assertTrue(countryOpt.isPresent());
	assertEquals(last, countryOpt.get());

  }

  @Test
  public void complexJoinWithGeneratedAvroTest() {

	Optional<CUlimitCustomerCollector> valOpt =
	  StreamsTestHelper.run(builder, properties)
		.sender("internal.customer", Tuple1.class, Customer.class)
		.rule(name("country_id"), constant(1), false)
		.send(10)
		.pipe()
		.sender("postgres.output.culimit", Key.class, Envelope.class)
		.rule(name("after"), setCountryId(), false)
		.send()
		.pipe()
		.keeper("merge.culimit_w_customers_aggr", Tuple1.class, CUlimitCustomerCollector.class)
		.find(defaultTuple1(), "merge.culimit_w_customers_aggr");


	assertTrue(valOpt.isPresent());
	assertEquals(10, valOpt.get().getValues().size());
  }

  private FieldUpdater<Value> setCountryId() {
	return oldVal -> {
	  oldVal.setCountryId(1);
	  return oldVal;
	};
  }

  private Country newCountryWithFixAskId() {
	int next = ++sequencer;
	return
	  Country
		.newBuilder()
		.setId(next)
		.setAskId(1)
		.setName("name" + next)
		.setSadkoId(next)
		.build();
  }

  private Tuple1 defaultTuple1() {
	return Tuple1.newBuilder().setValue1("1").build();
  }
}
