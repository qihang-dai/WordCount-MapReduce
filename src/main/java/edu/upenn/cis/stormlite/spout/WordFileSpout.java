package edu.upenn.cis.stormlite.spout;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class WordFileSpout extends FileSpout {
	static Logger log = LogManager.getLogger(WordFileSpout.class);
	@Override
	public String getFilename() {
		String storage = config.get("storage");
		String input  = config.get("input");
		String realName = null;
		log.info("storage: {}, input: {}", storage, input);
		if(storage == null || input == null){
			log.error("input and storage is null");
			return "words.txt";
		}
		String realDir = String.format("./%s/%s/", storage, input);
		String[] files = new File(realDir).list();
		for(String filename : files){
			String[] names = filename.split("\\.");
			if(names.length == 2){
				realName = names[0] + "." +names[1];
			}else{
				realName = names[0];
			}
		}
		String res = storage + realName;

		log.info(res);
		return res;
	}

}
