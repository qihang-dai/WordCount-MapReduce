package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis.stormlite.TopologyContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class WorkDemon implements Runnable{
    private static Logger logger = LogManager.getLogger(WorkDemon.class);

    List<String> results;
    TopologyContext context;
    int port;
    String masterUrl;
    String tmp = "http://%s/workerstatus?port=%s&status=%s&job=%s&keysRead=%s&keysWritten=%s&results=%s";
    volatile boolean run = true;
    String job = "errorJob";

    public void setJob(String job) {
        this.job = job;
    }

    public WorkDemon(TopologyContext context, int port, String master) {
        this.context = context;
        results = new ArrayList<>();
        this.port = port;
        masterUrl = master;
    }


    @Override
    public void run() {
        while (run){
            int keysRead = 0, keysWritten = 0;
            if(context.getState().equals(TopologyContext.STATE.MAPPING)){
                keysRead = context.getMapInputs();
                keysWritten = context.getMapOutputs();
            }else if(context.getState().equals(TopologyContext.STATE.MAPPING)){
                keysRead = context.getReduceInputs();
                keysWritten = context.getReduceOutputs();
            }
            String getUrl = String.format(tmp, masterUrl, port, context.getState(), job, keysRead, keysWritten, context.getResults().subList(0, 100));
            try {
                URL url = new URL(getUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.connect();
                logger.info(connection.getResponseMessage());
                connection.disconnect();
            } catch (IOException e) {
                logger.error("open connection wrong");
            }

        }

    }

    public void shutdown(){
        run = false;
    }
}
