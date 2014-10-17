package cn.edu.sdu.cs.starry.kafkaConsumer.util;

import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;

import java.util.Map;

/**
 * An interface for writing and recovering Kafka's offsets.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public interface IOffsetLogManager {
    public void tryToReconnect() throws ConsumerLogException;

    public void loadOffsetLog(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException;

    public void saveOffsets(Map<Integer, Long> consumeOffsetMap)
            throws ConsumerLogException;

    public void close();
}
