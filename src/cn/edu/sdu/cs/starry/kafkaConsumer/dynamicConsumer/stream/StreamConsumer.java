package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer.stream;

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

/**
 * A StreamConsumer consumes messages from Kafka and pushes them via {@link IMessageSender} without ack.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class StreamConsumer extends BaseConsumer {
    private static Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);
    private boolean shutdown;
    private IMessageSender messageSender;
    private int logFlushInterval;
    private int fetchRate;

    public StreamConsumer(String consumerName, String topic, Set<Integer> managedPartitionsSet, IMessageSender messageSender) throws ConsumerConfigException, ConsumerLogException {
        super(consumerName,topic, managedPartitionsSet);
        this.messageSender = messageSender;
        logFlushInterval = consumerConfig.getLogFlushInterval();
        fetchRate = consumerConfig.getFetchRate();
        shutdown = false;
    }

    @Override
    protected void initFetchOperator() throws ConsumerLogException {
        Set<Integer> partitionSet = new HashSet();
        partitionSet.addAll(managedPartitionsSet);
        fetchOperator = new StreamFetchOperator(topic,
                managedPartitionsSet,
                logManager, consumerName);
    }

    /**
     * Start fetching messages for stream from kafkaConsumer with given fetch rate
     * etc. NOT thread safe!!!!!
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException
     *
     */
    public void startFetchingAndPushing(boolean uptToDate, int fetchSize) throws KafkaCommunicationException {
        if (uptToDate) {
            setOffsets(System.currentTimeMillis());
        }
        int fetchTimes = 0;
        while (!shutdown) {
            try {
                Thread.sleep(fetchRate);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            List<KafkaMessage> messageAndOffsetList = fetchMessage(fetchSize);
            for (KafkaMessage message : messageAndOffsetList) {
                try {
                    messageSender.sendMessage(message.getMessage());
                } catch (Exception ex) {
                    LOG.error("Sending failed! " + ex.getMessage());
                    continue;
                }
            }
            if (messageAndOffsetList.size() > 0) {
                fetchTimes = (fetchTimes + 1) % logFlushInterval;
            }
            if (fetchTimes == 0 && messageAndOffsetList.size() > 0) {
                System.out.println("FLush");
                try {
                    fetchOperator.flushOffsets();
                } catch (ConsumerLogException e) {
                    LOG.warn("Flush offsets error!");
                    LOG.warn(e.getMessage());
                    fetchOperator.handleLogError();
                }
            }
            messageAndOffsetList.clear();
        }

    }
    @Override
    public void close() {
        shutdown = true;
        messageSender.close();
    }
}
