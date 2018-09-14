#### Description:
StreamsTestHelper serves for doing process unit tests for streams more easily.
It based on TopologyTestDriver and 
work in case if you need kafka streams, avro classes as model and schema-registry.

#### Properties:
* all your tests using this lib should be inherited from BaseStreamsTest
* build project to your local maven repo and add gradle dependency :)
```
testCompile("ru.gpb.als.streams:streams-test-support:0.1")
```
#### Usage examples:

```
    // generate records .
    List<ConsumerRecord<byte[], byte[]>> records = StreamsTestHelper
            .run(builder, properties)
            .sender("topic", () -> null, () -> newValue())
            .generate(10);
     
     // generate and send records to topic.
    StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send(10);       
        
     // generate , send and receive record  
    ProducerRecord<Key, Value> record = StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send()
        .pipe()
        .receiver("topic-next", Key.class, Value.class)
        .read();        
           
     // generate, send and try to find new value in store(for stores and ktables)      
    Optional<Value> record = StreamsTestHelper
        .run(builder, properties)
        .sender("topic", () -> null, () -> newValue())
        .send(10)
        .pipe()
        .keeper("topic-next",Key.class,Value.class)
        .find(new Key("1"));                          
```