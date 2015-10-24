package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;

/**
 * An interface for sending(pushing) messages.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public interface IMessageSender {
    public void sendMessage(KafkaMessage message) throws Exception;

    public void close();
}
