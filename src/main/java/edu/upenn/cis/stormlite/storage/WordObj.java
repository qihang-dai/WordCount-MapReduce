package edu.upenn.cis.stormlite.storage;

import com.sleepycat.persist.model.Entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Entity
public class WordObj implements Serializable {

    String key;
    List<String> values;

    public WordObj(String key) {
        this.key = key;
        this.values = new ArrayList<>();
    }

    public void addWord(String word){
        this.values.add(word);
    }

    public List<String> getValues(){return values;}


}
