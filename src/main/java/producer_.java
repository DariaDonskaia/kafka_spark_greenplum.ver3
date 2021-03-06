
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class producer_ {
    /*Properties Producer local

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";
    private  static Producer<Long,String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);



    }
*/

    public static void main(String... args) throws Exception {
        /* Producer local

        long index = 0;


            final Producer<Long, String> producer = createProducer();
            for (index = 0 ; index < 100; index++) {
            //Gen UUID
                UUID id1 = UUID.randomUUID();
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index,
                                "Hello!" + index + " "+ id1);

                RecordMetadata metadata = producer.send(record).get();


                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) ",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset());

            }

*/
        consumer_.run_consumer();
    }
}
