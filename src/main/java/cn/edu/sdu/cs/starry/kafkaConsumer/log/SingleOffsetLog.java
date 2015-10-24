package cn.edu.sdu.cs.starry.kafkaConsumer.log;

/**
 * A single log contains a global partition Id and an offset.<br/>
 * The valid logStr looks like "1:1000".<br/>
 * An item with negative partitionId means invalid.
 *
 * @author SDU.xccui
 */
public class SingleOffsetLog {
    private static final String SINGLE_LOG_SPLITTER = ":";
    private int partitionId;
    private long offset;

    public SingleOffsetLog(String logStr) {
        try {
            String[] logStrArray = logStr.split(SINGLE_LOG_SPLITTER);
            partitionId = Integer.valueOf(logStrArray[0]);
            offset = Long.valueOf(logStrArray[1]);
        } catch (Exception ex) {
            partitionId = -1;
        }
    }

    public SingleOffsetLog(int partitionId, long offset) {
        this.partitionId = partitionId;
        this.offset = offset;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getOffset() {
        return offset;
    }

    public String toString() {
        return "SingleOffsetLog [" + partitionId + SINGLE_LOG_SPLITTER + offset + "]";
    }
}
