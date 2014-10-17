package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

import java.io.Serializable;

/**
 * Broker host and port
 * @copyright
 *
 * @author SDU.xccui
 * @version 0.8.0
 */
public class BrokerInfo implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 3856989620386480110L;

    private String host;
    private int port;

    public BrokerInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public BrokerInfo(String host) {
        this(host, 9092);
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
        return host + ":" + port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

}
