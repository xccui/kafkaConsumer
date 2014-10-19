package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.batch;

import cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.BaseConsumer;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.BrokerInfo;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.ConsumerAndPartitions;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * Batch consumer with ack.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class BatchConsumer extends BaseConsumer {
    private static Logger LOG = LoggerFactory.getLogger(BatchConsumer.class);

    public BatchConsumer(String consumerName,String topic, Set<Integer> managedPartitionsSet)
            throws ConsumerConfigException, ConsumerLogException {
        super(consumerName,topic, managedPartitionsSet);
    }

    @Override
    protected void initFetchOperator() throws ConsumerLogException {
        Set<Integer> partitionSet = new HashSet();
        partitionSet.addAll(managedPartitionsSet);
        fetchOperator = new BatchFetchOperator(topic,
                managedPartitionsSet,
                logManager, consumerName);
    }

    /**
     * Acknowledge the consumed offset after consumed.
     *
     * @param ackMap
     */
    public void ackMessage(Map<Integer, Long> ackMap) {
        for (Entry<Integer, Long> entry : ackMap.entrySet()) {
            ((BatchFetchOperator) fetchOperator).ackConsumeOffset(
                    entry.getKey(), entry.getValue());
        }
        try {
            fetchOperator.flushOffsets();
        } catch (ConsumerLogException e) {
            LOG.warn("Flush offsets error!");
            LOG.warn(e.getMessage());
            fetchOperator.handleLogError();
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    public void resetToConsumedOffset() {
        ((BatchFetchOperator) fetchOperator).resetToConsumedOffset();
    }
}
