package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.BrokerInfo;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.ConsumerAndPartitions;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaErrorException;
import cn.edu.sdu.cs.starry.kafkaConsumer.log.FileLogManager;
import cn.edu.sdu.cs.starry.kafkaConsumer.log.IOffsetLogManager;
import cn.edu.sdu.cs.starry.kafkaConsumer.log.ZKLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * An abstract class for kafka consumer.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public abstract class BaseConsumer {
    private static Logger LOG = LoggerFactory
            .getLogger(BaseConsumer.class);

    protected ConsumerConfig consumerConfig;
    protected ConsumerPool consumerPool;
    protected BaseFetchOperator fetchOperator;
    protected Set<Integer> managedPartitionsSet;
    protected IOffsetLogManager logManager;
    protected String consumerName;
    protected String topic;

    /**
     * @param consumerName         a name to identify this consumer
     * @param managedPartitionsSet partition ids managed by this consumer
     * @throws ConsumerConfigException
     * @throws ConsumerLogException
     */
    public BaseConsumer(String consumerName, String topic, Set<Integer> managedPartitionsSet)
            throws ConsumerConfigException, ConsumerLogException {
        this.managedPartitionsSet = Collections.synchronizedSet(new TreeSet<Integer>());
        this.managedPartitionsSet.addAll(managedPartitionsSet);
        this.consumerName = consumerName;
        this.topic = topic;
        consumerConfig = new ConsumerConfig();
        consumerConfig.initConfig();// config should be initialized first
        String zkHosts = consumerConfig.getZkHosts();
        if (null != zkHosts) {
            LOG.info("Using ZKLogManager");
            logManager = new ZKLogManager(zkHosts, consumerName, topic);
        } else {
            LOG.info("Using FileLogManager");
            logManager = new FileLogManager(consumerConfig.getDataDir(), topic);
        }
        consumerPool = new ConsumerPool(consumerName, topic, consumerConfig);
        try {
            consumerPool.initConsumerPool(managedPartitionsSet);
        } catch (KafkaCommunicationException e) {
            LOG.error("can not connect to kafka, go to shutdown" + e.getMessage());
            System.exit(1);
        }
        initFetchOperator();
        fetchOperator.loadHistoryOffsets();
        Runtime.getRuntime().addShutdownHook(new ShutdownHandlerThread());
    }

    /**
     * Reconnect when encountered an exception.
     *
     * @throws KafkaCommunicationException
     */
    public void reconnect() throws KafkaCommunicationException {
        consumerPool.closeAllConsumer();
        consumerPool = new ConsumerPool(consumerName, topic, consumerConfig);
        consumerPool.initConsumerPool(managedPartitionsSet);
        LOG.warn("kafka consumer reconnected!! Perhaps encountered an error!");
    }

    protected abstract void initFetchOperator() throws ConsumerLogException;


    /**
     * Fetch a single message from kafkaConsumer by partitionId and offset. NOT thread
     * safe!!!!!
     *
     * @param partitionId
     * @param offset
     * @param fetchSize
     * @return
     * @throws java.io.IOException
     */
    public KafkaMessage fetchSingleMessage(int partitionId, long offset, int fetchSize)
            throws KafkaCommunicationException, KafkaErrorException {
        return fetchOperator.fetchSingleMessage(
                consumerPool.getConsumer(partitionId), partitionId, offset,
                fetchSize);
    }

    /**
     * Fetch message from Kafka with given fetch size.
     *
     * @param fetchSize the fetch size in bytes
     * @return fetched list for {@link KafkaMessage}
     */
    public List<KafkaMessage> fetchMessage(int fetchSize) throws KafkaCommunicationException {
        LOG.info("begin to fetch message");
        List<KafkaMessage> messageAndOffsetList = new LinkedList<>();//to store all messages
        List<KafkaErrorException> partitionsWithError = new LinkedList<>();
        for (Map.Entry<BrokerInfo, ConsumerAndPartitions> entry : getManagedPartitions().entrySet()) {
            Map<Integer, List<KafkaMessage>> messagesOnSingleBrokers = new TreeMap<>();//to store messages on brokers
            // fetch messages on each broker
            LOG.debug("Fetch broker " + entry.getKey().getHost());
            LOG.debug("partitionSet " + entry.getValue().partitionSet);
            partitionsWithError = fetchOperator.fetchMessage(
                    entry.getValue().consumer,
                    entry.getValue().partitionSet, fetchSize, messagesOnSingleBrokers);
            for (Map.Entry<Integer, List<KafkaMessage>> messageOnBroker : messagesOnSingleBrokers.entrySet()) {
                messageAndOffsetList.addAll(messageOnBroker.getValue());
            }
        }
        for (KafkaErrorException error : partitionsWithError) {
            LOG.error("Error while fetching messages from partition from " + error.getPartition(), error);
            LOG.error("Try to reset partition [{}] from topic [{}]", error.getPartition(), error.getTopic());
            consumerPool.clearOldPartitionInfo(error.getPartition()); //First clear the old partition information.
            consumerPool.getConsumer(error.getPartition());//Then find the partition again.
        }
        return messageAndOffsetList;
    }

    /**
     * Set all the managed offsets to the given time.
     *
     * @param time the time you want to set offsets to
     * @throws KafkaCommunicationException
     */
    protected void setAllOffsetsTo(long time) throws KafkaCommunicationException {
        for (Map.Entry<BrokerInfo, ConsumerAndPartitions> entry : consumerPool.managedPartitions.entrySet()) {
            fetchOperator.setSendOffsetsByTime(entry.getValue().consumer, entry.getValue().partitionSet, time);
        }
    }

    protected Map<BrokerInfo, ConsumerAndPartitions> getManagedPartitions() {
        return consumerPool.managedPartitions;
    }

    protected void setBatchOffsets() throws KafkaErrorException {
        for (Map.Entry<BrokerInfo, ConsumerAndPartitions> entry : consumerPool.managedPartitions.entrySet()) {
            fetchOperator.setBatchOffset(entry.getValue().consumer, entry.getValue().partitionSet);
        }
    }

    /**
     * Get this consumer topic
     */
    public String getTopic() {
        return topic;
    }

    public abstract void close();

    /**
     * Deal with shutdown signal
     *
     * @author xccui
     */
    private class ShutdownHandlerThread extends Thread {
        public void run() {
            /*try {                                            by sry
                LOG.info("Consumer will shut down!");
                fetchOperator.flushOffsets();
            } catch (ConsumerLogException e) {
                e.printStackTrace();
            } finally {
                fetchOperator.close();
                consumerPool.closeAllConsumer();
                close();
            }*/
            consumerPool.closeAllConsumer();
            close();
        }
    }
}
