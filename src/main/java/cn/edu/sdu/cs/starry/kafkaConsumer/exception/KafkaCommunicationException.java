package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * @author xccui
 */
public class KafkaCommunicationException extends Exception {
    public KafkaCommunicationException(String message) {
        super(message);
    }

    public KafkaCommunicationException(Throwable throwable) {
        super(throwable);
    }
}
