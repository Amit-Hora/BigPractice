package com.avrokafka.application;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.avrokafka.constants.AvrokafkaConstants;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class AvroKafkaApplication {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "nlxs5170.best-nl0114.slb.com:6667,nlxs5171.best-nl0114.slb.com:6667");

		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(AvrokafkaConstants.USER_SCHEMA);

		Schema.Parser parser2 = new Schema.Parser();
		Schema schema1 = parser2.parse(AvrokafkaConstants.USER_SCHEMA1);
		
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
		
		
		Properties cprops = new Properties();
		cprops.put("bootstrap.servers", "XX:9092");

		cprops.put("zookeeper.connect", "XX:2181");
		cprops.put("group.id", "g4");
		cprops.put("zookeeper.session.timeout.ms", "400");
		cprops.put("zookeeper.sync.time.ms", "200");
		cprops.put("auto.commit.interval.ms", "10000");
		cprops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		cprops.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		cprops.put("auto.offset.reset", "earliest");
		KafkaConsumer consumer = new KafkaConsumer<String, String>(cprops);
		 consumer.subscribe(Arrays.asList("avrotest4"));
		 for(;;){
		 /*sendMessage(producer, recordInjection, schema);
		 System.out.println("Message Sent......");*/
		readMessage(consumer, schema1);
		 }
		

	}

	static void sendMessage(KafkaProducer producer, Injection<GenericRecord, byte[]> recordInjection, Schema schema)
			throws InterruptedException, ExecutionException {

		GenericData.Record avroRecord = new GenericData.Record(schema);
		avroRecord.put("fname", "amit");
		avroRecord.put("lname", "singh");
		avroRecord.put("sal", 42);

		byte[] bytes = recordInjection.apply(avroRecord);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>("avrotest4", "users", bytes);

		producer.send(record).get();
	}

	static void readMessage(KafkaConsumer consumer, Schema schema)
			throws InterruptedException, ExecutionException {



		 Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		 ConsumerRecords<String,byte[]> avroRecords = consumer.poll(100l);
		 for (ConsumerRecord<String,byte[]> avroRecord : avroRecords){
			   GenericRecord record = recordInjection.invert(avroRecord.value()).get();

		         System.out.println("fname= " + record.get("fname")
		                 + ", lname= " + record.get("lname")
		                /* + ", sal=" + record.get("sal")*/);
		 }
      
	}
}
