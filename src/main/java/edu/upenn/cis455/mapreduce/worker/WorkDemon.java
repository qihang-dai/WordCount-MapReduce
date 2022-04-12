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
    TopologyContext context = new TopologyContext();
    int port;
    String masterUrl;
    String tmp = "http://%s/workerstatus?port=%s&status=%s&job=%s&keysRead=%s&keysWritten=%s&results=%s";
    static volatile boolean run = true;
    String job = "errorJob";

    public void setJob(String job) {
        this.job = job;
    }

    public void setContext(TopologyContext context) {
        this.context = context;
    }

    public WorkDemon(int port, String master) {
        results = new ArrayList<>();
        this.port = port;
        masterUrl = master;
    }

    public String formatResultToString(){
        List<String>  res = new ArrayList<>();
        for(String s : context.getResults()){
            res.add(s);
            if(res.size() > 100) break;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(String s : res){
            sb.append(s).append(",");
        }
        sb.append("]");
        logger.info(sb);
        return sb.toString();
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
            List<String>  res = new ArrayList<>();
            for(String s : context.getResults()){
                res.add(s);
                if(res.size() > 100) break;
            }

            if(masterUrl == null) masterUrl = "localhost:45555";
            String getUrl = String.format(tmp, masterUrl, port, context.getState(), job, keysRead, keysWritten, res.toString().replaceAll("\\s", ""));
            logger.info("url： {}", getUrl);
            try {
                URL url = new URL(getUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.connect();
                logger.info(connection.getResponseMessage());
                connection.disconnect();
                logger.info("Worker {} reported.", port);


            } catch (IOException  e) {
                logger.error("open connection wrong");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    public static void shutdown(){
        run = false;
    }
}
