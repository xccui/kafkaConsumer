package cn.edu.sdu.cs.starry.kafkaConsumer.dynamicConsumer;

import cn.edu.sdu.cs.starry.kafkaConsumer.entity.BrokerInfo;
import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Config loader for kafkaConsumer server and dynamic consumer.
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class ConsumerConfig {

    private static Logger LOG = LoggerFactory
            .getLogger(ConsumerConfig.class);

    // Properties config file paths
    private static final String KAFKA_CONSUMER_PROPERTIES_PATH = "conf/kafka8/kafkaConsumer.properties";

    // Properties Keys
    private static final String HOSTS_KEY = "brokers";
    private static final String TIME_OUT_KEY = "timeOut";
    private static final String BUFFER_SIZE_KEY = "bufferSize";
    private static final String FETCH_SIZE_KEY = "fetchSize";

    private static final String FETCH_RATE_KEY = "fetchRate";
    private static final String LOG_FLUSH_INTERVAL_KEY = "logFlushInterval";

    private static final String ZK_HOSTS_KEY = "zkHosts";
    private static final String DATA_DIR_KEY = "dataDir";
    private Properties kafkaConsumerProperties;

    // KafkaServer properties
    private List<BrokerInfo> brokers;

    // Static consumer properties
    private int timeOut;
    private int bufferSize;
    private int fetchSize;
    private int fetchRate;
    private int logFlushInterval;
    private String zkHosts;
    private String dataDir;
    // Default values
    private static final int LOG_FLUSH_INTERVAL_DEFAULT = 10;

    public ConsumerConfig() throws ConsumerConfigException {
        kafkaConsumerProperties = new Properties();
        brokers = new ArrayList<BrokerInfo>();
        loadPropertiesFiles();
    }

    /**
     * Initialize config properties before using them.
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException
     *
     */
    public void initConfig() throws ConsumerConfigException {
        parseServerConfig();
        parseStaticConsumerConfig();
    }

    public List<BrokerInfo> getBrokers() {
        return brokers;
    }

    public String getDataDir() {
        return dataDir;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getFetchRate() {
        return fetchRate;
    }

    public int getLogFlushInterval() {
        return logFlushInterval;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    /**
     * Load properties from files
     *
     * @throws cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerConfigException
     *
     */
    private void loadPropertiesFiles() throws ConsumerConfigException {
        LOG.info("Start loading properties...");
        LOG.info("KAFKA_CONSUMER_PROPERTIES_PATH: "
                + KAFKA_CONSUMER_PROPERTIES_PATH);
        try {
            kafkaConsumerProperties.load(new FileReader(
                    KAFKA_CONSUMER_PROPERTIES_PATH));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new ConsumerConfigException("Can not find file: "
                    + KAFKA_CONSUMER_PROPERTIES_PATH);
        } catch (IOException e) {
            e.printStackTrace();
            throw new ConsumerConfigException("Error reading file: "
                    + KAFKA_CONSUMER_PROPERTIES_PATH);
        }
    }

    private void parseServerConfig() throws ConsumerConfigException {
        String hostsStr = kafkaConsumerProperties.getProperty(HOSTS_KEY);
        checkPropertyExist(hostsStr, HOSTS_KEY, KAFKA_CONSUMER_PROPERTIES_PATH);
        String[] hostStrArray = hostsStr.split(",");
        for (String hostStr : hostStrArray) {
            String[] singleHost = hostStr.split(":");
            if (singleHost.length != 2) {
                throw new ConsumerConfigException("Invalid host: " + hostStr);
            }
            try {
                BrokerInfo hostPort = new BrokerInfo(singleHost[0],
                        Integer.valueOf(singleHost[1]));
                brokers.add(hostPort);
            } catch (NumberFormatException ex) {
                throw new ConsumerConfigException("Invalid port: "
                        + singleHost[1]);
            }
        }
        LOG.info("Loaded Kafka brokers:" + brokers);
    }

    private void parseStaticConsumerConfig() throws ConsumerConfigException {
        // timeOut
        String timeOutStr = kafkaConsumerProperties.getProperty(TIME_OUT_KEY);
        checkPropertyExist(timeOutStr, TIME_OUT_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        try {
            timeOut = Integer.valueOf(timeOutStr.trim());
        } catch (NumberFormatException ex) {
            throw new ConsumerConfigException("Invalid timeOut value: "
                    + timeOutStr);
        }
        checkIntValueGreaterThan(timeOut, 0, TIME_OUT_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        LOG.info("Using timeOut: " + timeOut);
        // bufferSize
        String bufferSizeStr = kafkaConsumerProperties
                .getProperty(BUFFER_SIZE_KEY);
        checkPropertyExist(bufferSizeStr, BUFFER_SIZE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        try {
            bufferSize = Integer.valueOf(bufferSizeStr.trim());
        } catch (NumberFormatException ex) {
            throw new ConsumerConfigException("Invalid bufferSize value: "
                    + bufferSizeStr);
        }
        checkIntValueGreaterThan(bufferSize, 0, BUFFER_SIZE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        LOG.info("Using bufferSize: " + bufferSize);
        // fetchSize
        String fetchSizeStr = kafkaConsumerProperties
                .getProperty(FETCH_SIZE_KEY);
        checkPropertyExist(fetchSizeStr, FETCH_SIZE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        try {
            fetchSize = Integer.valueOf(fetchSizeStr.trim());
        } catch (NumberFormatException ex) {
            throw new ConsumerConfigException("Invalid fetchSize value: "
                    + fetchSizeStr);
        }
        checkIntValueGreaterThan(fetchSize, 0, FETCH_SIZE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        LOG.info("Using fetchSize: " + fetchSize);
        // fetchRate
        String fetchRateStr = kafkaConsumerProperties
                .getProperty(FETCH_RATE_KEY);
        checkPropertyExist(fetchRateStr, FETCH_RATE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        try {
            fetchRate = Integer.valueOf(fetchRateStr.trim());
        } catch (NumberFormatException ex) {
            throw new ConsumerConfigException("Invalid fetchRate value: "
                    + fetchRateStr);
        }
        checkIntValueGreaterThan(fetchRate, 0, FETCH_RATE_KEY,
                KAFKA_CONSUMER_PROPERTIES_PATH);
        LOG.info("Loaded fetchRate: " + fetchRate);
        String logFlushIntervalStr = kafkaConsumerProperties.getProperty(
                LOG_FLUSH_INTERVAL_KEY,
                String.valueOf(LOG_FLUSH_INTERVAL_DEFAULT));
        try {
            logFlushInterval = Integer.valueOf(logFlushIntervalStr.trim());
            if (logFlushInterval < 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException ex) {
            LOG.error("Invalid logFlushInterval value: " + logFlushIntervalStr
                    + ", using default: " + LOG_FLUSH_INTERVAL_DEFAULT);
            logFlushInterval = LOG_FLUSH_INTERVAL_DEFAULT;
        }
        LOG.info("Loaded logFlushInterval: " + logFlushIntervalStr);
        // zkHosts
        zkHosts = kafkaConsumerProperties.getProperty(ZK_HOSTS_KEY);
        try {
            checkPropertyExist(zkHosts, ZK_HOSTS_KEY,
                    KAFKA_CONSUMER_PROPERTIES_PATH);
            LOG.info("Using " + ZK_HOSTS_KEY + ":" + zkHosts);
        } catch (ConsumerConfigException ex) {
            LOG.info(ZK_HOSTS_KEY + " not found. Try to find dataDir");
            // dataDir
            dataDir = kafkaConsumerProperties.getProperty(DATA_DIR_KEY);
            checkPropertyExist(dataDir, DATA_DIR_KEY,
                    KAFKA_CONSUMER_PROPERTIES_PATH);
            File file = new File(dataDir);
            if (!file.exists() && !file.mkdirs()) {
                throw new ConsumerConfigException("DataDir '" + dataDir
                        + "' is not valid");
            }
            LOG.info("Using dataDir: " + dataDir + " and FileLogManager");
        }

    }

    private void checkPropertyExist(String property, String propertyKey,
                                    String propertyFilePath) throws ConsumerConfigException {
        if (null == property || property.trim().length() == 0) {
            throw new ConsumerConfigException("Missing '" + propertyKey
                    + "' property in " + propertyFilePath);
        }
    }

    private void checkIntValueGreaterThan(int propertyValue,
                                          int greaterThanValue, String propertyKey, String propertyFilePath)
            throws ConsumerConfigException {
        if (propertyValue < greaterThanValue) {
            throw new ConsumerConfigException("Value in '" + propertyFilePath
                    + "' " + propertyKey + " = " + propertyValue
                    + " should be greater than " + greaterThanValue);
        }

    }
}
