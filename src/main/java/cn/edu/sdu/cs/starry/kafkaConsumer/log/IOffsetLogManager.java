package cn.edu.sdu.cs.starry.kafkaConsumer.log;

import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;

import java.util.Map;

/**
 * An interface for writing and recovering Kafka's offsets.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public interface IOffsetLogManager {
    void tryToReconnect() throws ConsumerLogException;

    void loadOffsetLog(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException;

    void saveOffsets(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException;

    void close();
}
