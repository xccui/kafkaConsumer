package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

import java.io.Serializable;

/**
 * Broker host and port
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class BrokerInfo implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 3856989620386480110L;

    public static final int DEFAULT_PORT = 9092;
    private final String host;
    private final int port;

    public BrokerInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Initialize broker information with {@link BrokerInfo#DEFAULT_PORT}.
     *
     * @param host broker ip or hostname
     */
    public BrokerInfo(String host) {
        this(host, DEFAULT_PORT);
    }

    @Override
    public boolean equals(Object o) {
        BrokerInfo other = (BrokerInfo) o;
        return host.equals(other.host) && port == other.port;
    }

    @Override
    public int hashCode() {
        return (host + port).hashCode();
    }

    @Override
    public String toString() {
        return "Kafka broker: " + host + ":" + port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

}
