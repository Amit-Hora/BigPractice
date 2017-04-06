package com.avrokafka.constants;

public class AvrokafkaConstants {

	public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"employee\","
            + "\"fields\":["
            + "  { \"name\":\"fname\", \"type\":\"string\" },"
            + "  { \"name\":\"lname\", \"type\":\"string\" },"
            + "  { \"name\":\"sal\", \"type\":\"int\" }"
            + "]}";
	
	
	public static final String USER_SCHEMA1 = "{"
            + "\"type\":\"record\","
            + "\"name\":\"employee\","
            + "\"fields\":["
            + "  { \"name\":\"fname\", \"type\":\"string\" },"
            + "  { \"name\":\"lname\", \"type\":\"string\" }"
           
            + "]}";
	
}
