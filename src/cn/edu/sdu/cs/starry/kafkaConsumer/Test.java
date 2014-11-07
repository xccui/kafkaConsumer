package cn.edu.sdu.cs.starry.kafkaConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.IMessageSender;
import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream.StreamConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;

import java.nio.ByteBuffer;
import java.util.*;

public class Test {
    public static void main(String args[]) throws ConsumerConfigException, ConsumerLogException, KafkaCommunicationException {
       // Test example = new Test();
       // long maxReads = Long.parseLong(args[0]);
       // String topic = args[1];
       // int partition = Integer.parseInt(args[2]);
       // List<String> seeds = new ArrayList<String>();
       // seeds.add(args[3]);
       // int port = Integer.parseInt(args[4]);
       // try {
       //     example.run(maxReads, topic, partition, seeds, port);
       // } catch (Exception e) {
       //     System.out.println("Oops:" + e);
       //     e.printStackTrace();
       // }
        Set<Integer> managedSet = new HashSet<Integer>();
        for(int i=0;i<30;i++){
            managedSet.add(i);
        }
        StreamConsumer consumer = new StreamConsumer("xccui","lytest123", managedSet,new IMessageSender() {
            @Override
            public void sendMessage(KafkaMessage message) throws Exception {
               System.out.println(new String(message.getMessage(),"UTF-8"));
            }

            @Override
            public void close() {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        consumer.startFetchingAndPushing(true, 1024*1024*10);
    }
}

