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
        HashMap<Integer, Long> fetchOffsetMap = new HashMap<Integer, Long>();
        Set<PartitionAndOffset> partitionAndOffsetSet = new HashSet<PartitionAndOffset>();
        for (int partitionId : partitionSet) {
            if (!sendOffsetMap.containsKey(partitionId)) {
                initializeOffset(consumer, partitionId);
            }
            fetchOffsetMap.put(partitionId, sendOffsetMap.get(partitionId));
            partitionAndOffsetSet.add(new PartitionAndOffset(partitionId, fetchOffsetMap.get(partitionId)));
        }
        Map<Integer, ByteBufferMessageSet> messageSetMap = new TreeMap();
        boolean noError = true;
        boolean connectionError = true;    
        while(connectionError){
            try {
            	LOG.info("fecth operator , begin do fetch");
                noError = doFetch(consumer, partitionAndOffsetSet, fetchSize, messageSetMap);
                if(!noError){
                	//if has error, it should wait 1 second. Otherwise, kafka-server will write log too fast.
                	//but if sleep too long here, it will be not realtime fetch
                	LOG.info("fecth with error , sleep 1 second");
                	try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    	LOG.info("Thread interrupted, return true");
                    	return true;
                    }
                }
                connectionError = false;
            } catch (Exception ex){
                LOG.info("Error while fetching message, will retry.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                	LOG.info("Thread interrupted, return true");
                	return true;
                }
            }
        }
        LOG.info("fecth operator , end do fetch");
        for (int partitionId : partitionSet) {
        	List<KafkaMessage> messageAndOffsetList = new LinkedList<>();// clear buffer list
        	ByteBufferMessageSet messageSet = messageSetMap.get(partitionId);
            long fetchOffset = fetchOffsetMap.get(partitionId);
            if(messageSet != null && messageSet.iterator().hasNext()){
                LOG.info("Fetched " + messageSet.sizeInBytes() + " bytes form partition " + partitionId + " on host "
                        + consumer.host() + " with offset " + fetchOffset);
                for (MessageAndOffset mo : messageSet) {
                	//LOG.info("mo.offset=" + mo.offset() + "  fetchOffset=" + fetchOffset);
                    if (mo.offset() < fetchOffset){
                    	continue;
                    }else{
                    	messageAndOffsetList.add(genKafkaMessage(mo.offset(), partitionId, mo
                                .message().payload()));
                        fetchOffset = mo.nextOffset();
                    }
                }
                LOG.info("Update offset for " + partitionId + " with offset " + fetchOffset);
                sendOffsetMap.put(partitionId, fetchOffset);
                messageOnBrokers.put(partitionId, messageAndOffsetList);
            }
        }
        LOG.info("fecth operator , finished do fetch");
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
        if(messageSet != null){
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
            //TODO to deal with fetching errors
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
