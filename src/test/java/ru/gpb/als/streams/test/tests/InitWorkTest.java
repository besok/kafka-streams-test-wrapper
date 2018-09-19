package ru.gpb.als.streams.test.tests;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import ru.gpb.als.model.Country;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.streams.test.BaseStreamsTest;
import ru.gpb.als.streams.test.StreamsTestHelper;
import ru.gpb.als.streams.test.data.StreamsUtils;
import ru.gpb.als.streams.test.helpers.StreamsTestHelperContext;
import ru.gpb.als.streams.test.helpers.generators.AvroGenerator;

import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.*;
import static ru.gpb.als.streams.test.data.StreamsUtils.*;
import static ru.gpb.als.streams.test.data.StreamsUtils.H.*;
import static ru.gpb.als.streams.test.helpers.ValueProducer.NULL_PRODUCER;

/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class InitWorkTest extends BaseStreamsTest {

  @Autowired
  private StreamsBuilder builder;
  @Autowired
  @Qualifier("streams")
  private Properties properties;
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
		.find(wrap("1"));
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
		.send(5).last().value();

	Optional<Country> countryOpt =
	  ctx
		.keeper(store, Tuple1.class, Country.class)
		.find(wrap(last.getAskId()));

	assertTrue(countryOpt.isPresent());
	assertEquals(last, countryOpt.get());

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

}
