package cn.edu.sdu.cs.starry.kafkaConsumer.entity;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-18
 * Time: 下午3:56
 * To change this template use File | Settings | File Templates.
 */
public class PartitionAndOffset {
    public int partition;
    public long offset;

    public PartitionAndOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

	@Override
	public String toString() {
		return "PartitionAndOffset [partition=" + partition + ", offset="
				+ offset + "]";
	}
}
