package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream;

/**
 * An interface for sending(pushing) messages.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public interface IMessageSender {
    public void sendMessage(byte[] message) throws Exception;

    public void close();
}
