package com.qiaoda.mongo;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnection {
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	
	public MongoConnection(){
		mongoClient = new MongoClient(Arrays.asList(
				new ServerAddress("192.168.6.117", 30000),
                new ServerAddress("192.168.6.118", 30000)));
		
		mongoDatabase = mongoClient.getDatabase("file_storage");
//		boolean auth = mongoDatabase.("root","123456");
//		MongoClientOptions.builder().build();
	}
	
	public void insertMg(Document doc){
		MongoCollection<Document> collection = mongoDatabase.getCollection("file_map");
		collection.insertOne(doc);
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}
	
	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}
	public void close(){
		mongoClient.close();
	}
}
