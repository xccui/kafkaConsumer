package cn.edu.sdu.cs.starry.kafkaConsumer.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-18
 * Time: 下午2:11
 * To change this template use File | Settings | File Templates.
 */
public class KafkaCommunicationException extends Exception {
    public KafkaCommunicationException(String message) {
        super(message);
    }

    public KafkaCommunicationException(Throwable throwable) {
        super(throwable);
    }
}
