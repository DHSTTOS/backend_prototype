package com;

public class main {

    public static void main(String[] args) {
        //init zookeper & kafka server

        //produce smthn
        System.out.println("Hello World!");

        MongoDB mongo = new MongoDB("kafka");

        MongoConsumer c =  new MongoConsumer(mongo,null,null,null);

    }
}