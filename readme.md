#### Description:
StreamsTestHelper makes easy test process for kafka-streams.
It based on TopologyTestDriver and 
work in case if you need kafka streams, avro classes as model and schema-registry.

#### Properties:
* all your tests using this lib should be inherited from BaseStreamsTest
* build project to your local maven repo and add gradle dependency :)
```
testCompile("ru.gpb.als.streams:streams-test-support:0.1")
```
#### Usage examples:
  
##### Generate and send records
* generate batch or single records
* generate and send records to topic.
   
```
    List<ConsumerRecord<byte[], byte[]>> records = StreamsTestHelper
            .run(builder, properties)
            .sender("topic", () -> null, () -> newValue())
            .generate(10);
     
    StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send(10);       
```

##### Send and receive records
* you can switch from sender to receiver or keeper using pipe() method.

```        
    ProducerRecord<Key, Value> record = StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send()
        .pipe()
        .receiver("topic-next", Key.class, Value.class)
        .read();        
           
    Optional<Value> record = StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send(10)
        .pipe()
        .keeper("topic-next",Key.class,Value.class)
        .find(new Key("1")); 
```

##### Avro generation
* if you have avro classes you can use avro generator
* using rules for processing fields 
  * using static import for FieldUpdaterPredicate or FieldUpdater you can simplify rules
  
```        
    List<ConsumerRecord<byte[], byte[]>> records = StreamsTestHelper
            .run(builder, properties)
            .sender("topic", Key.class, Value.class)
            .generate(10);
            
    List<ConsumerRecord<byte[], byte[]>> records = StreamsTestHelper
            .run(builder, properties)
            .sender("topic", Key.class, Value.class)
            .rule(
                field -> field.name().equals("field"), 
                v -> 10, false) 
            .send(10); 
  
    List<ConsumerRecord<byte[], byte[]>> records = StreamsTestHelper
            .run(builder, properties)
            .sender("topic", Key.class, Value.class)
            .rule(name("field"), constant(10), false) 
            .send(10);                    
                                             
```

#### License
This project is licensed under the terms of the MIT license.
