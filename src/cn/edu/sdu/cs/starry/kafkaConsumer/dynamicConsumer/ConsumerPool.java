package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.BrokerInfo;
import cn.edu.sdu.cs.starry.kafkaConsumer.entity.ConsumerAndPartitions;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.KafkaCommunicationException;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains a couple of kafkaConsumer simple consumers related to specific host and host
 * partition
 *
 * @author SDU.xccui
 */
public class ConsumerPool {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerPool.class);
    private static final String ID_LEADER_LOOKUP = "leaderLookup";
    protected Map<BrokerInfo, ConsumerAndPartitions> managedPartitions;//from broker to partitions it contains
    private Map<Integer, SimpleConsumer> consumerMap;
    private String topic;
    private List<BrokerInfo> brokerList;
    private int timeOut;
    private int bufferSize;
    private String myName;

    public ConsumerPool(String myName, String topic, ConsumerConfig consumerConfig) {
        consumerMap = new ConcurrentHashMap<>();
        managedPartitions = new ConcurrentHashMap<>();
        this.topic = topic;
        this.brokerList = consumerConfig.getBrokers();
        this.timeOut = consumerConfig.getTimeOut();
        this.bufferSize = consumerConfig.getBufferSize();
        this.myName = myName;
    }

    /**
     * Get the consumer response for the given id partition from a consumer map.
     * If the consumer is not contained, ask remote brokers.
     *
     * @param partitionId
     * @return the broker consumer that manage the given partition
     */
    public SimpleConsumer getConsumer(int partitionId) throws KafkaCommunicationException {
        SimpleConsumer consumer = consumerMap.get(partitionId);
        if (null == consumer) {
            PartitionMetadata meta = findLeader(brokerList, topic, partitionId);
            BrokerInfo brokerInfo = new BrokerInfo(meta.leader().host(), meta.leader().port());
            if (managedPartitions.containsKey(brokerInfo)) {
                consumer = managedPartitions.get(brokerInfo).consumer;
            } else {
                consumer = new SimpleConsumer(meta.leader().host(), meta.leader().port(), timeOut, bufferSize, myName + "_" + partitionId);
                //Set<Integer> partitionSet = new HashSet<Integer>();
                //partitionSet.add(partitionId);
                managedPartitions.put(brokerInfo, new ConsumerAndPartitions(consumer));
            }
            managedPartitions.get(brokerInfo).partitionSet.add(partitionId);
            consumerMap.put(partitionId, consumer);
        }
        return consumer;
    }

    /**
     * Find consumer for given partitions
     * @param managedPartitionSet
     * @throws KafkaCommunicationException
     */
    public void initConsumerPool(Set<Integer> managedPartitionSet) throws KafkaCommunicationException {
        for (int i : managedPartitionSet) {
            getConsumer(i);
        }
    }

    /**
     * When getting error, use this method to relocate consumer for partition
     * @param partitionId
     * @return
     * @throws KafkaCommunicationException
     */
    public SimpleConsumer relocateConsumer(int partitionId) throws KafkaCommunicationException {
        SimpleConsumer oldConsumer = consumerMap.remove(partitionId);
        BrokerInfo brokerInfo = new BrokerInfo(oldConsumer.host(), oldConsumer.port());
        oldConsumer.close();
        ConsumerAndPartitions consumerAndPartitions = managedPartitions.get(brokerInfo);
        consumerAndPartitions.partitionSet.remove(partitionId);
        if (consumerAndPartitions.partitionSet.isEmpty()) {
            managedPartitions.remove(brokerInfo);
        }
        return getConsumer(partitionId);
    }

    private PartitionMetadata findLeader(List<BrokerInfo> brokerList, String topic, int partition) throws KafkaCommunicationException {
        PartitionMetadata returnMetaData = null;
        for (BrokerInfo broker : brokerList) {
            SimpleConsumer consumer = null;
            try {
                LOG.info("Try to find leader for partition " + partition + " from broker " + broker.getHost());
                consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), timeOut, 64 * 1024, ID_LEADER_LOOKUP);
                List<String> topics = new ArrayList<>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                if(resp == null){
                	continue;
                }
                List<TopicMetadata> metaData = resp.topicsMetadata();
                if(metaData == null){
                	continue;
                }
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                        	if(part.leader() != null){
                        		 returnMetaData = part;
                                 LOG.info("Found leader " + part.leader().host() + " for partition " + partition);
                                 break;
                        	}   
                        }
                    }
                }
                if (null != returnMetaData) {
                    break;
                }
            } catch (Exception e) {
                LOG.warn("Error while finding leader for partition "+partition+" from broker "+broker.getHost() + ", will try next.");
                e.printStackTrace();
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (null == returnMetaData) {
            throw new KafkaCommunicationException("Can not find meta for " + topic + ":" + partition);
        }
        
        return returnMetaData;
    }


    /**
     * Close consumers in this pool
     */
    public synchronized void closeAllConsumer() {
        for (Entry<BrokerInfo, ConsumerAndPartitions> entry : managedPartitions.entrySet()) {
            entry.getValue().consumer.close();
        }
    }
}
