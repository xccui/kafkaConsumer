package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

import kafka.javaapi.consumer.SimpleConsumer;

/**
 * Description
 *
 * @author xccui
 */
public class KafkaErrorException extends Exception {
    private final SimpleConsumer consumer;
    private final String topic;
    private final int partition;
    private final String extraMessage;
    private final short errorCode;

    public KafkaErrorException(Throwable throwable, short errorCode, SimpleConsumer consumer, String topic, int partition, String extraMessage) {
        super(extraMessage, throwable);
        this.errorCode = errorCode;
        this.consumer = consumer;
        this.partition = partition;
        this.topic = topic;
        this.extraMessage = extraMessage;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public SimpleConsumer getConsumer() {
        return consumer;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getExtraMessage() {
        return extraMessage;
    }
}
