package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.KafkaMessage;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.PartitionAndOffset;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import cn.edu.sdu.cs.starry.kafkaConsumer.util.IOffsetLogManager;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
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
 * Message fetcher and logger
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
        sendOffsetMap = new TreeMap<Integer, Long>();
        this.logManager = logManager;
    }

    /**
     * Load history offsets from log file
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException
     *
     */
    public abstract void loadHistoryOffsets() throws ConsumerLogException;

    /**
     * Fetch messages using the given simple consumer and tracking offsets
     */
    public boolean fetchMessage(SimpleConsumer consumer, Set<Integer> partitionSet, int fetchSize, Map<Integer, List<KafkaMessage>> messageOnBrokers) throws KafkaCommunicationException {
        long fetchOffset = -1;
        PartitionAndOffset partitionAndOffset = null;
        Set<PartitionAndOffset> partitionAndOffsetSet = new HashSet<PartitionAndOffset>();
        for (int partitionId : partitionSet) {
            if (!sendOffsetMap.containsKey(partitionId)) {
                initializeOffset(consumer, partitionId);
            }
            fetchOffset = sendOffsetMap.get(partitionId);
            partitionAndOffset = new PartitionAndOffset(partitionId, fetchOffset);
            partitionAndOffsetSet.add(partitionAndOffset);
        }
        Map<Integer, ByteBufferMessageSet> messageSetMap = new TreeMap();
        boolean noError = doFetch(consumer, partitionAndOffsetSet, fetchSize, messageSetMap);
        List<KafkaMessage> messageAndOffsetList;
        ByteBufferMessageSet messageSet;
        for (int partitionId : partitionSet) {
            messageAndOffsetList = new LinkedList<>();// clear buffer list
            messageSet = messageSetMap.get(partitionId);
            if(null != messageSet){
                if (messageSet.iterator().hasNext()){
                    LOG.debug("Fetched " + messageSet.sizeInBytes() + " bytes form partition " + partitionId + " on host "
                            + consumer.host() + " with offset " + partitionAndOffset.offset);
                    for (MessageAndOffset mo : messageSet) {
                        if (mo.offset() < fetchOffset)
                            continue;
                        messageAndOffsetList.add(genKafkaMessage(fetchOffset, partitionId, mo
                                .message().payload()));
                        fetchOffset = mo.nextOffset();
                    }
                    LOG.debug("Update offset for " + partitionId + " with offset " + fetchOffset);
                    sendOffsetMap.put(partitionId, fetchOffset);
                    messageOnBrokers.put(partitionId, messageAndOffsetList);
                }
            }
        }
        return noError;
    }

    private void initializeOffset(SimpleConsumer consumer, int partitionId) throws KafkaCommunicationException {
        Set<Integer> partitionSet = new HashSet();
        partitionSet.add(partitionId);
        setToOffset(consumer, partitionSet, kafka.api.OffsetRequest.EarliestTime());
    }

    /**
     * Fetch single message by partitionId and offset
     */
    public KafkaMessage fetchSingleMessage(SimpleConsumer consumer, int partitionId, long offset, int fetchSize) {
        Set<PartitionAndOffset> partitionAndOffsetList = new HashSet();
        partitionAndOffsetList.add(new PartitionAndOffset(partitionId, offset));
        Map<Integer, ByteBufferMessageSet> messageSetMap = new TreeMap();
        doFetch(consumer, partitionAndOffsetList, fetchSize, messageSetMap);
        ByteBufferMessageSet messageSet = messageSetMap.get(partitionId);
        Iterator<MessageAndOffset> iterator = messageSet.iterator();
        if (iterator.hasNext()) {
            return genKafkaMessage(offset, partitionId, iterator.next()
                    .message().payload());
        }
        return null;
    }

    /**
     * Flush offsets to disk or other persistent medium
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException
     *
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


    protected void setToOffset(SimpleConsumer consumer, Set<Integer> partitionSet,
                               long time) throws KafkaCommunicationException {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        for (int partitionId : partitionSet) {
            requestInfo.put(new TopicAndPartition(topic, partitionId), new PartitionOffsetRequestInfo(time, 1));
        }
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
        if (offsetResponse.hasError()) {
            throw new KafkaCommunicationException(offsetResponse.toString());
        }
        for (int partitionId : partitionSet) {
            sendOffsetMap.put(partitionId, offsetResponse.offsets(topic, partitionId)[0]);
        }
    }


    private boolean doFetch(SimpleConsumer consumer,
                            Set<PartitionAndOffset> partitionAndOffsetSet, int fetchSize, Map<Integer, ByteBufferMessageSet> messageSetMap) {
        FetchRequestBuilder builder = new FetchRequestBuilder().clientId(clientName);

        for (PartitionAndOffset partitionAndOffset : partitionAndOffsetSet) {
            builder.addFetch(topic, partitionAndOffset.partition, partitionAndOffset.offset, fetchSize);
        }
        FetchRequest req = builder.build();
        FetchResponse fetchResponse = consumer.fetch(req);
        for (PartitionAndOffset partitionAndOffset : partitionAndOffsetSet) {
            if (fetchResponse.errorCode(topic, partitionAndOffset.partition) == ErrorMapping.NoError()) {
                messageSetMap.put(partitionAndOffset.partition, fetchResponse.messageSet(topic, partitionAndOffset.partition));
            }
        }
        if (fetchResponse.hasError()) {
            return false;
        }
        return true;
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
