package ru.gpb.als.streams.test.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import postgres.output.culimit.Envelope;
import postgres.output.culimit.Key;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.model.collectors.CUlimitCustomerCollector;
import ru.gpb.als.model.collectors.CountryCustomerCollector;

import javax.annotation.PostConstruct;
import java.util.Objects;


/**
 * Created by Boris Zhguchev on 23/08/2018
 */
@Slf4j
@Component
public class CUlimitWorker {

  @Autowired
  private StreamsBuilder builder;

  @PostConstruct
  public void process() {

    KStream<Key, Envelope> culimits = builder.stream("postgres.output.culimit");
    KTable<Tuple1, CountryCustomerCollector> customersByCountry =
        builder.table("internal.customers_group_by_country");

    culimits
        .map(this::map)
        .groupByKey()
        .aggregate(this::createCollector, StreamsUtils.H::lastWin)
        .leftJoin(customersByCountry, this::join,Materialized.as("merge.culimit_w_customers_aggr"))
        .toStream()
        .peek(StreamsUtils.mark("culimit-by-country")::peek)
        .to("merge.culimit_w_customers_aggr");


  }

  private KeyValue<Tuple1, CUlimitCustomerCollector> map(Key k, Envelope e) {
    Tuple1 key = StreamsUtils.H.wrap(String.valueOf(e.getAfter().getCountryId()));

    CUlimitCustomerCollector val = CUlimitCustomerCollector
        .newBuilder()
        .setKey(e.getAfter())
        .setProperty("ru.gpb.als.streams.test.data.CUlimitWorker")
        .build();
    return new KeyValue<>(key, val);
  }

  private CUlimitCustomerCollector join(CUlimitCustomerCollector left, CountryCustomerCollector right) {
    if (!Objects.isNull(right))
      left.getValues().addAll(right.getValues());
    return left;
  }

  private CUlimitCustomerCollector createCollector() {
    return CUlimitCustomerCollector.newBuilder().setProperty("ru.gpb.als.streams.test.data.CUlimitWorker").build();
  }
}
