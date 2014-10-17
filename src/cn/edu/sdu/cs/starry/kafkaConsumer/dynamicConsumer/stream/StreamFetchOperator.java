package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream;

import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.BaseFetchOperator;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.util.IOffsetLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.Set;

/**
 * Message fetcher and logger for {@link StreamConsumer}.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class StreamFetchOperator extends BaseFetchOperator {
    private static Logger LOG = LoggerFactory.getLogger(StreamFetchOperator.class);

    public StreamFetchOperator(String topic, Set<Integer> managedPartitionSet, IOffsetLogManager logManager, String clientName)
            throws ConsumerLogException {
        super(topic, managedPartitionSet, logManager, clientName);
    }

    @Override
    public void loadHistoryOffsets() throws ConsumerLogException {
        for (Integer partitionId : managedPartitionSet) {
            sendOffsetMap.put(partitionId, 0L);
        }
        logManager.loadOffsetLog(sendOffsetMap);
    }

    @Override
    public void flushOffsets() throws ConsumerLogException {
        logManager.saveOffsets(sendOffsetMap);
    }

    @Override
    public void close() {
        LOG.info("StreamFetchOperator close\n=========Offset map===========\n");
        for (Entry<Integer, Long> entry : sendOffsetMap.entrySet()) {
            LOG.info("********\t" + entry.getKey() + ":" + entry.getValue());
        }
        logManager.close();
    }

    @Override
    public void handleLogError() {
        LOG.warn("StreamFetchOperator encountered an error!");
        LOG.info("=======Current offset map=========\n");
        for (Entry<Integer, Long> entry : sendOffsetMap.entrySet()) {
            LOG.warn(entry.getKey() + ":" + entry.getValue());
        }
        try {
            logManager.tryToReconnect();
        } catch (ConsumerLogException e) {
            e.printStackTrace();
        }
    }

}
