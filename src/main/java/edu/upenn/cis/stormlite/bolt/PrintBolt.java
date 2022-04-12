package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();
	String outputDir;
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
	TopologyContext context;

	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public boolean execute(Tuple input) {
		if (!input.isEndOfStream()){
			System.out.println(getExecutorId() + ": " + input.toString());
			log.info("outputdir: {}", outputDir);
			String key = input.getStringByField("key");
			String val = input.getStringByField("value");
			String line = String.format("(%s,%s)\n", key, val);
			FileWriter fileWriter = null;
			try {
				fileWriter = new FileWriter(outputDir,true);
			} catch (IOException e) {
				log.error("fw error");
			}
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			try {
				bufferedWriter.write(line);
				bufferedWriter.close();
//				context.incSendOutputs(key);
				context.addResult(line);
			} catch (IOException e) {
				log.error("bw error");
				e.printStackTrace();
			}
		}
		context.setState(TopologyContext.STATE.IDLE);

		return true;
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
		this.context = context;
		String storage = stormConf.get("storage");
		String input  = stormConf.get("output");

		log.info("storage: {}, output: {}", storage, input);
		if(storage == null || input == null){
			log.error("input and storage is null");
			return;
		}
		outputDir = String.format("./%s/%s/", storage, input) + "output.txt";
		try {
			Files.deleteIfExists(Paths.get(outputDir));
		} catch (IOException e) {
			e.printStackTrace();
			log.error("deleteIfExists error");
		}
		try {
			Files.createFile(Paths.get(outputDir));
		} catch (IOException e) {
			e.printStackTrace();
			log.error("create file error");
		}

	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
