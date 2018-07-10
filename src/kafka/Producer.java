//$Id$
package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class Producer extends Thread {
public static void main(String[] args)
{
	String value;
	 // Check arguments length value
  //  Scanner sc=new Scanner(System.in);
//    System.out.println("Enter the topic");
    //Assign topicName to string variable
  //  String topicName = sc.next().toString();
    
    // create instance for properties to access producer configs   
    Properties props = new Properties();
    
    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092");
    
    //Set acknowledgements for producer requests.      
    props.put("acks", "all");
    
    //If the request fails, the producer can automatically retry,
    props.put("retries", 0);
    
    //Specify buffer size in config
    props.put("batch.size", 16384);
    
    //Reduce the no of requests less than 0   
    props.put("linger.ms", 1);
    
    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
    props.put("buffer.memory", 33554432);
    
    props.put("key.serializer", 
       "org.apache.kafka.common.serialization.StringSerializer");
       
    props.put("value.serializer", 
       "org.apache.kafka.common.serialization.StringSerializer");
    
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
    
    List<String> li=new ArrayList<String>();
    li.add("aaa");
    li.add("YOGESH");
    li.add("sample");
    
   for(String topic:li)
   {
    for(int i = 0; i < 40; i++)
    {
    	value="value - "+i;
    	ProducerRecord<String,String> rec=new ProducerRecord<String,String>(topic,Integer.toString(i),value); 
    	
       producer.send(rec);
    }
   }
             System.out.println("Message sent successfully");
             producer.close();

             
	}
}
