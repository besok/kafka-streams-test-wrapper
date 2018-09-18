package ru.gpb.als.streams.test.data;// 2018.08.02

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.kafka.streams.KeyValue;
import ru.gpb.als.model.Tuple1;
import ru.gpb.als.service.Envelope;
import ru.gpb.als.service.Status;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Boris Zhguchev
 */
@Slf4j
public class StreamsUtils {

  public static class H {
    public static boolean isRejected(Tuple1 k, Envelope<?, ?> env) {
      return env.getStatus() == Status.REJECT;
    }

    public static boolean isSuccessed(Tuple1 k, Envelope<?, ?> env) {
      return env.getStatus() != Status.REJECT;
    }

    public static <K, V> V lastWin(K key, V last, V prev) {
      return last;
    }

    public static <K, V> V firstWin(K key, V last, V prev) {
      return prev;
    }
    public static Tuple1 wrap(String v) {
      return Tuple1.newBuilder().setValue1(v).build();
    }

    public static Tuple1 wrap(Object v) {
      return Tuple1.newBuilder().setValue1(v.toString()).build();
    }

    public static Tuple1 wrap(CharSequence v) {
      return Tuple1.newBuilder().setValue1(v).build();
    }

    public static Optional<String> unwrap(Tuple1 tuple1) {
      return Optional.of(String.valueOf(tuple1.getValue1()));
    }
  }

  public static Wrapper field(String field) {
    return new Wrapper(field);
  }
  public static <K, V> Peeker<K, V> mark(String mark) {
    return new Peeker<>(mark);
  }
  public static <I, RK> Extractor<I, RK> field(Function<I, RK> fn) {
    return new Extractor<>(fn);
  }

  public static class Extractor<I, RK>  {
    private Function<I, RK> fn;

    Extractor(Function<I, RK> fn) {
      this.fn = fn;
    }


    public KeyValue<Tuple1, I> newKVFromV(Object key, I ent) {
      return new KeyValue<>(H.wrap(fn.apply(ent)), ent);
    }
    public <K> KeyValue<K, RK> newKV(K key, I ent) {
      return new KeyValue<>(key, fn.apply(ent));
    }
  }
  public static class Peeker<K, V> {
    private final String m;

    private Peeker(String mark) {
      this.m = mark;
    }

    public KeyValue<K, V> peek(K key, V val) {
      log.info("[{}] => k[{}] , v[{}]", m.toUpperCase(), key, val);
      return new KeyValue<>(key, val);
    }

  }
  public static class Wrapper {
    private String field;

    Wrapper(String field) {
      this.field = field;
    }

    public <T extends SpecificRecordBase> Tuple1 wrap(Object k, T ent) {
      return StreamsUtils.H.wrap(((Utf8) ent.get(field)).toString());
    }

    public <T extends SpecificRecordBase> KeyValue<Tuple1, T> newKV(Object k, T ent) {
      return new KeyValue<>(wrap(k, ent), ent);
    }
    @SuppressWarnings("unchecked")
    private <I extends SpecificRecordBase, O extends SpecificRecordBase> O get(I ent, String field) {
      return (O) ent.get(field);
    }
  }


}
