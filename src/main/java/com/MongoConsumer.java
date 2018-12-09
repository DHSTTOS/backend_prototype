package com;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

//for GSON
import java.lang.reflect.Type;

//For polling
import java.time.Duration;

import java.util.Arrays;
import java.util.List;

//for consumer creation
import java.util.Properties;

//project hasn't even started and we're already doing hacky shit

public class MongoConsumer {

    private KafkaConsumer<String, String> consumer;

    private Duration pollingTimeOut = Duration.ofMillis(100);

    private String[] topics;

    private MongoDB db;

    public MongoConsumer(MongoDB database,Properties properties, String[] topics,String db)
    {
        this.db = database;

        this.topics = topics;

        //TODO: hear properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        //create the new consumer for the props
        consumer = new KafkaConsumer<>(props);

        //TODO: diff between realtime and normal records.
        //TODO: loop for all topics

        //we do NOT subsrice to a topic, but initialize the first parition to the start and read from here, assuring we'll readd everything
        //this will be done once, to get all topics on to the db
        TopicPartition topicPartition = new TopicPartition("test", 0);
        List partitions = Arrays.asList(topicPartition);
        //manually assign the partition
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);




        ListenForRecords();


    }

    //TODO: should we check if records already exist?
    void ListenForRecords()
    {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(pollingTimeOut);


            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s, partition = %d%n", record.offset(), record.key(), record.value(), record.partition());

                Type type = new TypeToken<PacketRecord>() {}.getType();
                Gson gson = new Gson();

                //convert it into a java object
                PacketRecord incomingRecord = gson.fromJson(record.value(), type);
                //set the offset as ID in the DB
                incomingRecord.setoffset(Long.toString(record.offset()));

                addRecordToDB(incomingRecord);

                //db.AddRecordToCollection(db.getCollection("test"),incomingRecord);

                //System.out.println(incomingRecord.toString());

                //ADD to mongoDB

            }
        }
    }

    private void addRecordToDB(PacketRecord record)
    {
        db.AddRecordToCollection(db.getCollection("test"),record);
    }


}
