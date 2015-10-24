package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.PartitionAndOffset;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaErrorException;
import cn.edu.sdu.cs.starry.kafkaConsumer.log.IOffsetLogManager;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Fetch messages and log offsets.
 *
 * @author SDU.xccui
 */
public abstract class BaseFetchOperator {
    private static Logger LOG = LoggerFactory
            .getLogger(BaseFetchOperator.class);

    protected String topic;
    protected Map<Integer, Long> sendOffsetMap;
    protected IOffsetLogManager logManager;
    protected String clientName;
    protected Set<Integer> managedPartitionSet;

    public BaseFetchOperator(String topic, Set<Integer> managedPartitionSet, IOffsetLogManager logManager, String clientName)
            throws ConsumerLogException {
        this.topic = topic;
        this.managedPartitionSet = managedPartitionSet;
        this.clientName = clientName;
        this.sendOffsetMap = Collections.synchronizedMap(new TreeMap<Integer, Long>());
        this.logManager = logManager;
    }

    /**
     * Load history offsets from log file
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException
     */
    public abstract void loadHistoryOffsets() throws ConsumerLogException;

    /**
     * Fetch messages using the given simple consumer and log offsets.
     *
     * @param consumer
     * @param partitionSet
     * @param fetchSize
     * @param messageOnBrokers
     * @return
     * @throws KafkaCommunicationException
     */
    public List<KafkaErrorException> fetchMessage(SimpleConsumer consumer, Set<Integer> partitionSet, int fetchSize, Map<Integer, List<KafkaMessage>> messageOnBrokers) {
        HashMap<Integer, Long> fetchOffsetMap = new HashMap<>();
        Set<PartitionAndOffset> partitionAndOffsetSet = new HashSet<>();
        for (int partitionId : partitionSet) {
            if (!sendOffsetMap.containsKey(partitionId)) {
                initializeOffset(consumer, partitionId);
            }
            fetchOffsetMap.put(partitionId, sendOffsetMap.get(partitionId));
            partitionAndOffsetSet.add(new PartitionAndOffset(partitionId, sendOffsetMap.get(partitionId)));
        }
        final Map<Integer, ByteBufferMessageSet> messageSetMap = new TreeMap<>();
        List kafkaErrorList = doSingleConsumerFetch(consumer, partitionAndOffsetSet, fetchSize, messageSetMap);
        List<KafkaMessage> messageAndOffsetList;
        for (int partitionId : partitionSet) {
            messageAndOffsetList = new LinkedList<>();
            ByteBufferMessageSet messageSet = messageSetMap.get(partitionId);
            long fetchOffset = fetchOffsetMap.get(partitionId);
            if (null != messageSet && messageSet.iterator().hasNext()) {
                LOG.debug("Fetched " + messageSet.sizeInBytes() + " bytes form partition " + partitionId + " on host "
                        + consumer.host() + " with offset " + fetchOffset);
                for (MessageAndOffset mo : messageSet) {
                    LOG.debug("partition: [{}], fetchOffset: [{}], MessageOffset: [{}]", partitionId, fetchOffset, mo.offset());
                    if (mo.offset() < fetchOffset) {
                        //Drop messages whose offset is smaller thant the fetch offset.
                        continue;
                    } else {
                        messageAndOffsetList.add(genKafkaMessage(mo.offset(), partitionId, mo
                                .message().payload()));
                        //Update fetch offset.
                        fetchOffset = mo.nextOffset();
                    }
                }
                LOG.debug("Update offset for " + partitionId + " with offset " + fetchOffset);
                sendOffsetMap.put(partitionId, fetchOffset);
                messageOnBrokers.put(partitionId, messageAndOffsetList);
            }
        }
        return kafkaErrorList;
    }

    private void initializeOffset(SimpleConsumer consumer, int partitionId) {
        Set<Integer> partitionSet = new HashSet();
        partitionSet.add(partitionId);
        setSendOffsetsByTime(consumer, partitionSet, kafka.api.OffsetRequest.EarliestTime());
    }

    /**
     * Fetch single message by partitionId and offset
     */
    public KafkaMessage fetchSingleMessage(SimpleConsumer consumer, int partitionId, long offset, int fetchSize) throws KafkaErrorException {
        Set<PartitionAndOffset> partitionAndOffsetList = new HashSet();
        partitionAndOffsetList.add(new PartitionAndOffset(partitionId, offset));
        Map<Integer, ByteBufferMessageSet> messageSetMap = new TreeMap();
        doSingleConsumerFetch(consumer, partitionAndOffsetList, fetchSize, messageSetMap);
        ByteBufferMessageSet messageSet = messageSetMap.get(partitionId);
        if (messageSet != null) {
            Iterator<MessageAndOffset> iterator = messageSet.iterator();
            if (iterator.hasNext()) {
                return genKafkaMessage(offset, partitionId, iterator.next()
                        .message().payload());
            }
        }
        return null;
    }

    /**
     * Flush offsets to disk or other persistent medium
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException
     */
    public abstract void flushOffsets() throws ConsumerLogException;

    /**
     * Close this operator.
     */
    public abstract void close();

    /**
     * What to do when comes consumer log error.
     */
    public abstract void handleLogError();


    protected void setSendOffsetsByTime(SimpleConsumer consumer, Set<Integer> partitionSet,
                                        long time) {
        for (int partitionId : partitionSet) {
            try {
                sendOffsetMap.put(partitionId, fetchOffset(consumer, topic, partitionId, time));
            } catch (KafkaErrorException e) {
                LOG.warn("Can not fetch offset for partition " + partitionId, e);
            }
        }
    }

    /**
     * Fetch the offset of the given time.
     *
     * @param consumer
     * @param topic
     * @param partitionId
     * @param time
     * @return
     * @throws KafkaErrorException
     */
    public long fetchOffset(SimpleConsumer consumer, String topic, int partitionId, long time) throws KafkaErrorException {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        requestInfo.put(new TopicAndPartition(topic, partitionId), new PartitionOffsetRequestInfo(time, 1));
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
        if (offsetResponse.hasError()) {
            throw new KafkaErrorException(ErrorMapping.exceptionFor(offsetResponse.errorCode(topic, partitionId)), offsetResponse.errorCode(topic, partitionId), consumer, topic, partitionId, "fetchOffset, time = " + time);
        }
        return offsetResponse.offsets(topic, partitionId)[0];
    }

    protected void setBatchOffset(SimpleConsumer consumer, Set<Integer> partitionSet) throws KafkaErrorException {
        for (int partitionId : partitionSet) {
            long offset = -1;
            if (!sendOffsetMap.containsKey(partitionId)) {
                offset = fetchOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime());
                sendOffsetMap.put(partitionId, offset);
            } else if (sendOffsetMap.get(partitionId) < offset) {
                sendOffsetMap.put(partitionId, offset);
            }
        }
    }

    private List<KafkaErrorException> doSingleConsumerFetch(SimpleConsumer consumer,
                                                            Set<PartitionAndOffset> partitionAndOffsetSet, int fetchSize, Map<Integer, ByteBufferMessageSet> messageSetMap) {
        FetchRequestBuilder builder = new FetchRequestBuilder().clientId(clientName);
        List<KafkaErrorException> exceptionList = new LinkedList<>();
        for (PartitionAndOffset partitionAndOffset : partitionAndOffsetSet) {
            builder.addFetch(topic, partitionAndOffset.partition, partitionAndOffset.offset, fetchSize);
        }
        FetchRequest req = builder.build();
        FetchResponse fetchResponse = consumer.fetch(req);
        for (PartitionAndOffset partitionAndOffset : partitionAndOffsetSet) {
            short errorCode = fetchResponse.errorCode(topic, partitionAndOffset.partition);
            if (errorCode == ErrorMapping.NoError()) {
                messageSetMap.put(partitionAndOffset.partition, fetchResponse.messageSet(topic, partitionAndOffset.partition));
            } else {//With error
                LOG.error("Fetch data error: Error Code :[{}], topic :[{}], partition :[{}]",
                        errorCode, topic, partitionAndOffset.partition, ErrorMapping.exceptionFor(errorCode));
                try {
                    throw ErrorMapping.exceptionFor(errorCode);
                } catch (OffsetOutOfRangeException e) {
                    e.printStackTrace();
                    LOG.error("OffsetOutOfRangeException detected. Will handle this automatically.");
                    //fetch earliest offset.
                    LOG.error("Current fetch offset : [{}]", partitionAndOffset.offset);
                    long earliestOffset = -1;
                    long latestOffset = -1;
                    try {
                        earliestOffset = fetchOffset(consumer, topic, partitionAndOffset.partition, kafka.api.OffsetRequest.EarliestTime());
                        latestOffset = fetchOffset(consumer, topic, partitionAndOffset.partition, kafka.api.OffsetRequest.LatestTime());
                    } catch (KafkaErrorException ex) {
                        ex.printStackTrace();
                        exceptionList.add(new KafkaErrorException(ex, errorCode, consumer, topic, partitionAndOffset.partition, "handle offsetOutOfRange "));
                    }
                    LOG.error("Request Offset : [{}]", partitionAndOffset.offset);
                    LOG.error("Earliest Offset : [{}]", earliestOffset);
                    LOG.error("Latest Offset : [{}]", latestOffset);
                    //only set current fetch offset to earliest offset if possible. DO NOTHING otherwise.
                    if (earliestOffset > 0 && partitionAndOffset.offset < earliestOffset) {
                        LOG.error("Set partition : [{}] offset from [{}] to [{}]", partitionAndOffset.partition, partitionAndOffset.offset, earliestOffset);
                        LOG.warn("Drop offset : [{}]", earliestOffset - partitionAndOffset.offset);
                        //If the request offset is lower than the earliest, set it to the earliest.
                        sendOffsetMap.put(partitionAndOffset.partition, earliestOffset);
                    } else if (latestOffset > 0 && partitionAndOffset.offset > latestOffset) {
                        //If the request offset is greater than the latest, set it to the latest.
                        LOG.error("Set partition : [{}] offset from [{}] to [{}]", partitionAndOffset.partition, partitionAndOffset.offset, latestOffset);
                        sendOffsetMap.put(partitionAndOffset.partition, latestOffset);
                    } else {
                        LOG.error("Can not get the valid offsets");
                        exceptionList.add(new KafkaErrorException(e, errorCode, consumer, topic, partitionAndOffset.partition,
                                "Invalid offsets: earliest = " + earliestOffset + "; latest = " + latestOffset));
                    }
                    //TODO handle other exceptions here
                } catch (Throwable t) {
                    //Can not handle error, pack and return it.
                    exceptionList.add(new KafkaErrorException(t, errorCode, consumer, topic, partitionAndOffset.partition, "fetchMessage: offset = " + partitionAndOffset.offset));
                }
            }
        }
        return exceptionList;
    }

    /**
     * Gen kafkaConsumer message from base offset and payload byteBuffer
     *
     * @param offset
     * @param partitionId
     * @param buffer
     * @return
     */

    private KafkaMessage genKafkaMessage(long offset, int partitionId,
                                         ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        KafkaMessage message = new KafkaMessage(ret, partitionId, offset);
        return message;
    }

}
