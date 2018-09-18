package ru.gpb.als.streams.test.helpers.generators;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.*;

import static java.lang.String.*;


/**
 * Created by Boris Zhguchev on 18/09/2018
 */
public class AvroGenerator<T extends SpecificRecordBase> implements Generator<T> {

  protected Schema schema;
  protected T instance;
  protected Map<Schema.Type, GeneratorLogic> genByTypesMap;

  public AvroGenerator(Class<T> clzz) {

    try {
      this.instance = clzz.newInstance();
      this.schema = this.instance.getSchema();
    } catch (InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
    }
    fillMap();

  }

  @Override
  public T generate() {
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Object fieldVal = generateByType(field);
      instance.put(field.name(), fieldVal);
    }

    return instance;
  }

  public T generatedValue() {
    return instance;
  }


  private class GeneratorLogic<V> implements GeneratorByType<V> {
    private GeneratorByType<V> firstGen;
    private FieldUpdater<V> thenGen;

    public GeneratorLogic(GeneratorByType<V> firstGen, FieldUpdater<V> thenGen) {
      this.firstGen = firstGen;
      this.thenGen = thenGen;
    }

    public void setThenGen(FieldUpdater<V> thenGen) {
      this.thenGen = thenGen;
    }

    @Override
    public V generate(Schema.Field field) {
      return thenGen.update(firstGen.generate(field), field);
    }

    @Override
    public V generate(Schema schema, Schema.Field field) {
      return thenGen.update(firstGen.generate(schema,field),field);
    }
  }

  private interface GeneratorByType<V> {
    V generate(Schema.Field field);

    default V generate(Schema schema, Schema.Field field) {
      return generate(field);
    }
  }

  // FIXME: 9/18/2018 Не работает если мыспускаемся ниже топа - класскастэкс!
  public interface FieldUpdater<V> {
    V update(V val, Schema.Field field);

    static <V> FieldUpdater<V> EMPTY() {
      return (v, f) -> v;
    }

  }

  protected class NullGen implements GeneratorByType<Object> {

    @Override
    public Object generate(Schema.Field field) {
      return null;
    }
  }

  protected class BooleanGen implements GeneratorByType<Boolean> {

    private Random random = new Random();

    @Override
    public Boolean generate(Schema.Field field) {
      return random.nextInt(1) == 0;
    }
  }

  protected class DoubleGen implements GeneratorByType<Double> {

    private Random random = new Random();

    @Override
    public Double generate(Schema.Field field) {
      return random.nextDouble();
    }
  }

  protected class FloatGen implements GeneratorByType<Float> {

    private Random random = new Random();

    @Override
    public Float generate(Schema.Field field) {
      return random.nextFloat();
    }
  }

  protected class LongGen implements GeneratorByType<Long> {

    private Random random = new Random();

    @Override
    public Long generate(Schema.Field field) {
      return random.nextLong();
    }
  }

  protected class IntGen implements GeneratorByType<Integer> {

    private Random random = new Random();

    @Override
    public Integer generate(Schema.Field field) {
      return random.nextInt();
    }
  }

  protected class BytesGen implements GeneratorByType<byte[]> {

    private Random random = new Random();

    @Override
    public byte[] generate(Schema.Field field) {
      byte[] bytes = new byte[1000];
      random.nextBytes(bytes);
      return bytes;
    }
  }

  protected class StringGen implements GeneratorByType<String> {
    private Random random = new Random();

    @Override
    public String generate(Schema.Field field) {
      int rand = random.nextInt(1000);
      String name = field.name();
      char[] chars = new char[50];
      for (int i = 0; i < chars.length; i++) {
        chars[i] = (char) (random.nextInt(26) + 'a');
      }

      return join("-", name, valueOf(rand), valueOf(chars));
    }

  }

  protected class UnionGen implements GeneratorByType<Object> {

    @Override
    public Object generate(Schema.Field field) {
      return generate(field.schema().getTypes().get(1), field);
    }

    @Override
    public Object generate(Schema schema, Schema.Field field) {
      return generateBy(schema, field);
    }
  }

  protected class EnumGen<En extends Enum<En>> implements GeneratorByType<Enum<En>> {
    private Random random = new Random();

    @Override
    public Enum<En> generate(Schema.Field field) {
      try {
        List<String> enumList = field.schema().getEnumSymbols();
        @SuppressWarnings("unchecked")
        Class<En> aClass = (Class<En>) Class.forName(field.schema().getFullName());
        String name = enumList.get(random.nextInt(enumList.size()));
        return Enum.valueOf(aClass, name);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  protected class RecordGen<R extends SpecificRecordBase> implements GeneratorByType<R> {

    @Override
    public R generate(Schema.Field field) {
      return generate(field.schema(), field);
    }

    @SuppressWarnings("unchecked")
    @Override
    public R generate(Schema schema, Schema.Field field) {
      try {
        @SuppressWarnings("unchecked")
        R instance = (R) Class.forName(schema.getFullName()).newInstance();
        List<Schema.Field> fields = instance.getSchema().getFields();
        for (Schema.Field f : fields) {
          Object resField = generateByType(f);
          instance.put(f.name(), resField);
        }

        return instance;

      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  protected class MapGen<Km, Vm> implements GeneratorByType<HashMap<Km, Vm>> {
    @Override
    public HashMap<Km, Vm> generate(Schema.Field field) {
      return new HashMap<>();
    }

    @Override
    public HashMap<Km, Vm> generate(Schema schema, Schema.Field field) {
      return generate(field);
    }
  }

  protected class ArrayGen<El> implements GeneratorByType<ArrayList<El>> {
    @Override
    public ArrayList<El> generate(Schema.Field field) {
      ArrayList<El> arrs = new ArrayList<>();
      Schema elSchema = field.schema().getElementType();
      El objVal = (El) generateBy(elSchema, field);
      arrs.add(objVal);

      return arrs;
    }
  }

  protected Object generateByType(Schema.Field f) {
    return generateBy(f.schema(), f);
  }

  protected Object generateBy(Schema schema, Schema.Field field) {
    switch (schema.getType()) {
      case STRING:
      case UNION:
        return genByTypesMap.get(schema.getType()).generate(field);
      case INT:
      case ARRAY:
      case MAP:
      case ENUM:
      case LONG:
      case NULL:
      case BOOLEAN:
      case BYTES:
      case FLOAT:
      case DOUBLE:
      case RECORD:
        return genByTypesMap.get(schema.getType()).generate(schema, field);
      default:
        return null;
    }
  }

  public <V> void setUpdaterFor(Schema.Type type, FieldUpdater<V> updater){
    genByTypesMap.get(type).setThenGen(updater);
  }

  private void fillMap() {
    genByTypesMap = new HashMap<>();
    genByTypesMap.put(Schema.Type.INT, new GeneratorLogic<>(new IntGen(), FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.ARRAY, new GeneratorLogic<>(new ArrayGen<>(), FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.MAP, new GeneratorLogic<>(new MapGen<>(), FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.UNION, new GeneratorLogic<>(new UnionGen(), FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.ENUM, new GeneratorLogic<>(new EnumGen<>(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.STRING, new GeneratorLogic<>(new StringGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.LONG, new GeneratorLogic<>(new LongGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.NULL, new GeneratorLogic<>(new NullGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.BOOLEAN, new GeneratorLogic<>(new BooleanGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.BYTES, new GeneratorLogic<>(new BytesGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.FLOAT, new GeneratorLogic<>(new FloatGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.DOUBLE, new GeneratorLogic<>(new DoubleGen(),FieldUpdater.EMPTY()));
    genByTypesMap.put(Schema.Type.RECORD, new GeneratorLogic<>(new RecordGen<>(),FieldUpdater.EMPTY()));
  }
}
