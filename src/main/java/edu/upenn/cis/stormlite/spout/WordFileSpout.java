package edu.upenn.cis.stormlite.spout;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
		String direc = new File(realDir).getAbsolutePath();

		log.info("absolute storage: {}", direc);
		String[] files = new File(realDir).list();
		for(String filename : files){
			String[] names = filename.split("\\.");
			if(names.length == 1){
				realName = names[0];
			}else{
				realName = names[0] + "." +names[1];

			}
		}
		String res = realDir + realName;

		log.info(res);
		return res;
	}

}
