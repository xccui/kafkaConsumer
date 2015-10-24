package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

/**
 * @author SDU.xccui
 */
public class PartitionAndOffset {
    public final int partition;
    public final long offset;

    public PartitionAndOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "PartitionAndOffset [partition = " + partition + ", offset = "
                + offset + "]";
    }
}
