package ru.gpb.als.streams.test.context.generators;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created by Boris Zhguchev on 28/09/2018
 */
public class ArrayGeneratorTest {

  @Test
  public void generateArrayString() {
    ArrayGenerator<String> generator = new ArrayGenerator<>();
    ArrayList<String> arrayList = generator.generate(Schema.createArray(Schema.create(Schema.Type.STRING)));

    assertEquals(1,arrayList.size());
    assertEquals(String.class,arrayList.get(0).getClass());

  }


  @Test
  public void generateArrayNumber() {
    ArrayGenerator<Integer> generator = new ArrayGenerator<>();
    ArrayList<Integer> intList = generator.generate(Schema.createArray(Schema.create(Schema.Type.INT)));

    assertEquals(1,intList.size());
    assertEquals(Integer.class,intList.get(0).getClass());

  }

  // TODO: 9/28/2018  переписать тест без экспектед
  @Test(expected = AvroRuntimeException.class)
  public void generateArrayRecord() {
    Schema record = Schema.createRecord("", "", "", false);

    ArrayGenerator<?> generator =new ArrayGenerator<>();
    ArrayList<?> recordList = generator.generate(record);

    assertEquals(1,recordList.size());
  }


  @Test(expected = AvroRuntimeException.class)
  public void generateArrayNullFailed() {
    ArrayGenerator<Object> g =  new ArrayGenerator<>();
    ArrayList<Object> generate = g.generate(Schema.create(Schema.Type.NULL));
    System.out.println(generate.size());
  }





}