package edu.upenn.cis.stormlite.storage;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BerkeleyDB {
    private static Logger logger = LogManager.getLogger(BerkeleyDB.class);

    private EnvironmentConfig environmentConfig;
    private Environment environment;

    private DatabaseConfig databaseConfig;
    private Database catalogDB;
    private StoredClassCatalog catalog;

    private Database wordDB;
    private StoredSortedMap<String, WordObj> wordMap;

    private String directory;

    public BerkeleyDB(String directory) {
        this.directory = directory;
        Config(directory);
    }

    public void Config(String directory){
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTransactional(true);
        environmentConfig.setAllowCreate(true);
        this.environment = new Environment(new File(directory), environmentConfig);

        databaseConfig = new DatabaseConfig();
        databaseConfig.setTransactional(true);
        databaseConfig.setAllowCreate(true);

        catalogDB = environment.openDatabase(null, "catalog", databaseConfig);
        catalog = new StoredClassCatalog(catalogDB);

        TupleBinding<String> key = TupleBinding.getPrimitiveBinding(String.class);
        EntryBinding<WordObj> value = new SerialBinding<>(catalog, WordObj.class);

        wordDB = environment.openDatabase(null, "wordDB", databaseConfig);
        wordMap = new StoredSortedMap(wordDB, key, value, true);
    }

    public void addKeyValue(String key, String value){
        WordObj wordObj= wordMap.getOrDefault(key ,new WordObj(key));
        wordObj.addWord(value);
        wordMap.put(key, wordObj);
    }

    public Set<String> keySet(){
        return wordMap.keySet();
    }

    public WordObj getWordObj(String key){
        return wordMap.get(key);
    }
    public void close() throws DatabaseException {
        wordMap.clear();
        logger.info("start close DBs");
        catalogDB.close();
        logger.info("catLogdb close");
        wordDB.close();
        logger.info("userdb close");
        environment.close();
    }


}
