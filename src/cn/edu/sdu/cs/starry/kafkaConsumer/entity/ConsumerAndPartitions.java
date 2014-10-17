package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashSet;
import java.util.Set;

/**
 * Copyright
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class ConsumerAndPartitions {
    public SimpleConsumer consumer;
    public Set<Integer> partitionSet;

    public ConsumerAndPartitions(SimpleConsumer consumer) {
        this.consumer = consumer;
        partitionSet = new HashSet<Integer>();
    }
}
