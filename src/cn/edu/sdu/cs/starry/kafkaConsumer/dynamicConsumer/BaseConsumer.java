package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.BrokerInfo;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.ConsumerAndPartitions;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.util.FileLogManager;
import cn.edu.sdu.cs.starry.kafkaConsumer.util.IOffsetLogManager;
import cn.edu.sdu.cs.starry.kafkaConsumer.util.ZKLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This is an abstract class for consumers.
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
    protected  String topic;
    /**
     * @param consumerName         a name to identify this consumer
     * @param managedPartitionsSet partition ids managed by this consumer
     * @throws ConsumerConfigException
     * @throws ConsumerLogException
     */
    public BaseConsumer(String consumerName,String topic, Set<Integer> managedPartitionsSet)
            throws ConsumerConfigException, ConsumerLogException {
        this.managedPartitionsSet = managedPartitionsSet;
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
            logManager = new FileLogManager(consumerConfig.getDataDir(),topic);
        }
        consumerPool = new ConsumerPool(consumerName,topic, consumerConfig);
        try {
            consumerPool.initConsumerPool(managedPartitionsSet);
        } catch (KafkaCommunicationException e) {
            e.printStackTrace();
        }
        initFetchOperator();
        fetchOperator.loadHistoryOffsets();
        Runtime.getRuntime().addShutdownHook(new ShutdownHandlerThread());
    }

    /**
     * Reconnect when comes an exception, especially for IOException
     */
    public void reconnect() {
        consumerPool.closeAllConsumer();
        consumerPool = new ConsumerPool(consumerName, topic, consumerConfig);
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
            throws IOException, KafkaCommunicationException {
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
        List<KafkaMessage> messageAndOffsetList = new LinkedList<>();//to store all messages
        List<Integer> outDatedPartitionList = new LinkedList<>();
        for (Map.Entry<BrokerInfo, ConsumerAndPartitions> entry : getManagedPartitions().entrySet()) {
            Map<Integer, List<KafkaMessage>> messagesOnSingleBrokers = new TreeMap<>();//to store messages on brokers
            // fetch messages on each broker
            boolean noError = fetchOperator.fetchMessage(
                    entry.getValue().consumer,
                    entry.getValue().partitionSet, fetchSize, messagesOnSingleBrokers);
            for (Map.Entry<Integer, List<KafkaMessage>> messageOnBroker : messagesOnSingleBrokers.entrySet()) {
                messageAndOffsetList.addAll(messageOnBroker.getValue());
            }
            if (!noError){
                //add partition with fetching errors to out date list
                outDatedPartitionList.addAll(entry.getValue().partitionSet) ;
            }

        }
        for (int partition : outDatedPartitionList) {
            //relocate all partitions with fetching errors
            consumerPool.relocateConsumer(partition);
        }
        return messageAndOffsetList;
    }
    /**
     * Set offsets to the given time.
     *
     * @param time the time you want to set offsets to
     * @throws KafkaCommunicationException
     *
     */
    protected void setOffsets(long time) throws KafkaCommunicationException {
        for (Map.Entry<BrokerInfo, ConsumerAndPartitions> entry : consumerPool.managedPartitions.entrySet()) {
            fetchOperator.setToOffset(entry.getValue().consumer, entry.getValue().partitionSet, time);
        }
    }
    protected Map<BrokerInfo, ConsumerAndPartitions> getManagedPartitions() {
        return consumerPool.managedPartitions;
    }


    /**
     * Get this consumer topic*/
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
