package com;

import com.mongodb.MongoClient;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

//TODO: be able to filter out doccuments
//TODO: so mongodb filter
public class MongoDB{

    public MongoClient client;
    public MongoDatabase db;

    public  MongoDB(String database)
    {

        client = new MongoClient();
        db = client.getDatabase(database);

    }

    public MongoCollection<Document> getCollection(String collection)
    {
        return db.getCollection(collection);
    }

    //TODO: generalize Packetrecord to record
    public void AddRecordToCollection( MongoCollection<Document> collection, PacketRecord record)
    {
        try{
            collection.insertOne(record.getAsDocument());
            System.out.println("added new entry at: " + record.getOffset());
        }
        catch (MongoWriteException ex)
        {
            System.out.println("an entry with this offset already exists %n at offset: " + record.getOffset() );
        }
    }


}
