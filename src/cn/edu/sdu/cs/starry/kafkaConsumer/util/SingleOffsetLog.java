package cn.edu.sdu.cs.starry.kafkaConsumer.util;

/**
 * A single log contains a global partition Id and an offset. If partitionId=-1,
 * it means this log is invalid.
 * 
 * 
 * @author SDU.xccui
 * 
 */
public class SingleOffsetLog
{
	private static final String SINGLE_LOG_SPLITTER = ":";
	private int partitionId;
	private long offset;

	public SingleOffsetLog(String logStr)
	{
		String[] logStrArray = logStr.split(SINGLE_LOG_SPLITTER);
		try
		{
			partitionId = Integer.valueOf(logStrArray[0]);
			offset = Long.valueOf(logStrArray[1]);
		}
		catch (NumberFormatException ex)
		{
			partitionId = -1;
		}
	}

	public SingleOffsetLog(int partitionId, long offset)
	{
		this.partitionId = partitionId;
		this.offset = offset;
	}

	public int getPartitionId()
	{
		return partitionId;
	}

	public long getOffset()
	{
		return offset;
	}

	public String toString()
	{
		return partitionId + SINGLE_LOG_SPLITTER + offset;
	}
}
