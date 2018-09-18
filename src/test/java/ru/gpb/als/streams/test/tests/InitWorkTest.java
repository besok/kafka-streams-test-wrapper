package ru.gpb.als.streams.test.tests;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import ru.gpb.als.model.Country;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.streams.test.BaseStreamsTest;
import ru.gpb.als.streams.test.StreamsTestHelper;
import ru.gpb.als.streams.test.data.StreamsUtils;
import ru.gpb.als.streams.test.helpers.generators.AvroGenerator;

import java.util.Optional;
import java.util.Properties;

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


  @Test
  public void shouldLastWinTest() {
    Optional<Country> country =
        StreamsTestHelper.run(builder, properties)
            .sender("internal.country", NULL_PRODUCER, this::newCountryWithFixAskId)
            .send(20)
            .pipe()
            .keeper("internal.country_group_by_ask", Tuple1.class, Country.class)
            .find(StreamsUtils.H.wrap("1"));

    Assert.assertTrue(country.isPresent());

    --sequencer;
    Assert.assertEquals(newCountryWithFixAskId(), country.get());

  }

  @Test
  public void testAvroGenerator() {
    ConsumerRecord<Tuple1, Country> last =
        StreamsTestHelper.run(builder, properties)
            .sender("internal.country", Tuple1.class, Country.class)
            .send(5).last();

    Optional<Country> country =
        StreamsTestHelper.run(builder, properties)
            .keeper("internal.country_group_by_ask", Tuple1.class, Country.class)
            .find(StreamsUtils.H.wrap(last.value().getAskId()));

    Assert.assertTrue(country.isPresent());

    --sequencer;
    Assert.assertEquals(last.value(), country.get());

  }


  private int sequencer = 0;

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
