package ru.gpb.als.streams.test.data;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.gpb.als.model.Tuple1;

import javax.annotation.PostConstruct;

/**
 * Created by Boris Zhguchev on 26/09/2018
 */
@Component
public class CountryFilterWorker {
  @Autowired
  private StreamsBuilder b;

  @PostConstruct
  public void process(){
	KStream<Tuple1,Tuple1> stream = b.stream("simple-stream");
  }

}
