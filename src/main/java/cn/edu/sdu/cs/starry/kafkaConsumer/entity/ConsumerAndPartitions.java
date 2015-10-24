package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author SDU.xccui
 * @version 0.8.0
 */
public class ConsumerAndPartitions {
    public final SimpleConsumer consumer;
    public final Set<Integer> partitionSet;

    public ConsumerAndPartitions(SimpleConsumer consumer) {
        this.consumer = consumer;
        partitionSet = Collections.synchronizedSet(new HashSet<Integer>());
    }
}
