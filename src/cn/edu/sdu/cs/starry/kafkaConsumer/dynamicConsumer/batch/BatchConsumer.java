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
     * Fetch messages from Kafka with default fetch size.
     *
     * @return fetched list for {@link KafkaMessage}
     * @throws KafkaCommunicationException
     */
    public List<KafkaMessage> fetchMessage() throws KafkaCommunicationException {
        return fetchMessage(this.fetchSize);
    }

    /**
     * Fetch message from Kafka with given fetch size.
     *
     * @param fetchSize the fetch size in bytes
     * @return fetched list for {@link KafkaMessage}
     * @throws java.io.IOException if fetch failed due to e.g. I/O error
     */
    public List<KafkaMessage> fetchMessage(int fetchSize) {
        List<KafkaMessage> messageAndOffsetList = new LinkedList();
        for (Entry<BrokerInfo, ConsumerAndPartitions> entry : getManagedPartitions().entrySet()) {
            Map<Integer, List<KafkaMessage>> messagesOnSingleBroker = new TreeMap<>();
            try {
                boolean noError = fetchOperator.fetchMessage(
                        entry.getValue().consumer,
                        entry.getValue().partitionSet, fetchSize, messagesOnSingleBroker);
                for (Entry<Integer, List<KafkaMessage>> partitionOnSingleBroker: messagesOnSingleBroker.entrySet()) {
                    messageAndOffsetList.addAll(partitionOnSingleBroker.getValue());
                }
                if (!noError) {
                    for (int partition : entry.getValue().partitionSet) {
                        consumerPool.relocateConsumer(partition);
                    }
                }
            } catch (KafkaCommunicationException e) {
                e.printStackTrace();
            }
        }
        return messageAndOffsetList;
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
