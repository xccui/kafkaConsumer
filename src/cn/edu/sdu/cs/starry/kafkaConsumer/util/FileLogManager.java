package cn.edu.sdu.cs.starry.kafkaConsumer.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import cn.edu.sdu.cs.starry.kafkaConsumer.exception.ConsumerLogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file based implementation of log manager
 * 
 * @author SDU.xccui
 * 
 */
public class FileLogManager implements IOffsetLogManager {
	private static Logger logger = LoggerFactory.getLogger(FileLogManager.class);
	private static final String CONSUME_LOG_FILE_NAME = "consume_offset_log";
	private String logDir;

	private File consumeLogFile;
	private FileWriter consumeLogWriter;

	public FileLogManager(String dataDir, String topic) {
		this.logDir = dataDir + "/log/" + topic + "_" + "/";
		consumeLogFile = new File(logDir + CONSUME_LOG_FILE_NAME);
	}

	public void loadOffsetLog(Map<Integer, Long> consumeOffsetMap)
			throws ConsumerLogException {
		if (consumeLogFile.exists() && consumeLogFile.canRead()) {
			try {
				BufferedReader consumerLogReader = new BufferedReader(
						new FileReader(consumeLogFile));
				logger.info("Start loading history offset from file '"
						+ consumeLogFile + "'");
				String consumerLogStr = consumerLogReader.readLine();
				while (consumerLogStr != null) {
					SingleOffsetLog singleLog = new SingleOffsetLog(
							consumerLogStr);
					if (singleLog.getPartitionId() != -1) {// Valid log
						Long oldOffset = consumeOffsetMap.get(singleLog
								.getPartitionId());
						if (oldOffset == null) {
							oldOffset = 0l;
						}
						if (singleLog.getOffset() > oldOffset) {
							consumeOffsetMap.put(singleLog.getPartitionId(),
									singleLog.getOffset());
							logger.debug("For partition: "
									+ singleLog.getPartitionId()
									+ " offset reached: "
									+ singleLog.getOffset());
						}
					}
					consumerLogStr = consumerLogReader.readLine();
				}
				consumerLogReader.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new ConsumerLogException(
						"Error reading consume offset log file '"
								+ consumeLogFile + "'");
			}
		} else {
			try {
				consumeLogFile.getParentFile().mkdirs();
				consumeLogFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				throw new ConsumerLogException(
						"Error creating consume offset log file '"
								+ consumeLogFile + "'");

			}
			logger.info("Offset log file '" + consumeLogFile + "' do not exist");
		}
	}

	private void initLogFileWriter() throws ConsumerLogException {
		try {
			consumeLogWriter = new FileWriter(consumeLogFile);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ConsumerLogException(
					"Error creating writer for offset log file '"
							+ consumeLogFile + "'");
		}
	}

	public void saveOffsets(Map<Integer, Long> consumeOffsetMap)
			throws ConsumerLogException {
		if (null == consumeLogWriter) {
			initLogFileWriter();
		}
		try {
			for (Entry<Integer, Long> singleOffset : consumeOffsetMap
					.entrySet()) {
				consumeLogWriter.write(new SingleOffsetLog(singleOffset
						.getKey(), singleOffset.getValue()).toString() + "\n");
			}
			consumeLogWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
			throw new ConsumerLogException(
					"Error while writing consume offset log");
		}
	}

	public void close() {
		if (null != consumeLogWriter) {
			try {
				consumeLogWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void tryToReconnect() throws ConsumerLogException {
		// TODO Auto-generated method stub

	}
}
