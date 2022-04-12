package edu.upenn.cis455.mapreduce.master;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.PrintBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.spout.WordFileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.worker.WorkDemon;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static spark.Spark.*;

public class MasterServer {
    private static final Logger logger = LogManager.getLogger(MasterServer.class);

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    private static Map<String, Map<String, String>> workersMap = new HashMap<>();

    public static void workerReport(){
        get("/workerstatus", (request, response) -> {
            String ip = request.ip();
            String port = request.queryParams("port");
            String ipPort = ip + ":" +port;
            String status = request.queryParams("status");
            String job = request.queryParams("job");
            String keysRead = request.queryParams("keysRead");
            String keysWritten = request.queryParams("keysWritten");
            String results = request.queryParams("results");
            String timeStamp = new Timestamp(System.currentTimeMillis()).toString();
            Map<String, String> singleWorkerInfo = new HashMap<>();
            singleWorkerInfo.put("ip", ip);
            singleWorkerInfo.put("port", port);
            singleWorkerInfo.put("ip-port", ipPort);
            singleWorkerInfo.put("status", status);
            singleWorkerInfo.put("job",job);
            singleWorkerInfo.put("keysRead", keysRead);
            singleWorkerInfo.put("keysWritten", keysWritten);
            singleWorkerInfo.put("results", results);
            singleWorkerInfo.put("timStamp", timeStamp);
            workersMap.put(ipPort, singleWorkerInfo);
            logger.info("CurWorker listern on: {}", ipPort);
            response.status(200);
            return singleWorkerInfo.toString();
        });
    }

    public static void registerStatusPage() {
        get("/status", (request, response) -> {
            response.type("text/html");
            String head = "<html><head><title>Master</title></head>\n" +
                    "<body><h1>Hi, I am the master!</h1><br/>\n";
            StringBuilder res = new StringBuilder(head);

            int n = 0;
            for(Map<String, String> singleWorker: workersMap.values()){
                String id = n + ": ";
                String port = "port=" + singleWorker.get("port");
                String status = "status=" + singleWorker.get("status");
                String job = "job=" + singleWorker.get("job");
                String keysRead = "keysRead=" + singleWorker.get("keysRead");
                String keysWritten = "keysWritten=" + singleWorker.get("keysWritten");
                String results = "results=" + singleWorker.get("results");
                String info = String.join(", ", port, status, job, keysRead, keysWritten, results);
                res.append(id).append(info).append("<br>");

            }


            String form = "<form method=\"POST\" action=\"/submitjob\">\n" +
                    "    Job Name: <input type=\"text\" name=\"jobname\" value=\"WordCount\"/><br/>\n" +
                    "    Class Name: <input type=\"text\" name=\"classname\" value=\"edu.upenn.cis455.mapreduce.job.WordCount\"/><br/>\n" +
                    "    Input Directory: <input type=\"text\" name=\"input\" value=\"input\"/><br/>\n" +
                    "    Output Directory: <input type=\"text\" name=\"output\" value=\"\"/><br/>\n" +
                    "    Map Threads: <input type=\"text\" name=\"map\" value=\"2\"/><br/>\n" +
                    "    Reduce Threads: <input type=\"text\" name=\"reduce\" value=\"1\"/><br/>\n" +
                    "    <input type=\"submit\" value=\"Submit Job\" value=\"John\"> <br/>\n"+
                    "</form>";

            res.append(form);
            res.append("</body></html>");
            return res.toString();

        });

    }


    public static void launch(){
        post("/submitjob", (request, response) -> {
            logger.info("************ get info from request  ***************");
            String jobName = request.queryParams("jobname");
            String className = request.queryParams("classname");
            String input = request.queryParams("input");
            String output = request.queryParams("output");
            String map = request.queryParams("map");
            String reduce = request.queryParams("reduce");

            List<String> workerList = new ArrayList<>();
            for(String workerIpPort : workersMap.keySet()){
                workerList.add(workerIpPort);
            }

            logger.info("workerList: {}", workerList);
            if(workerList.size() == 0) return "No worker";
            logger.info("************ Creating the Config  ***************");

            Config config = new Config();

            //workerLiST
            logger.info("worker list: {}", workerList.toString());
            config.put("workerList", workerList.toString());

            // Job name
            config.put("job", jobName);

            // Class with map function
            config.put("mapClass", className);
            // Class with reduce function
            config.put("reduceClass", className);

            // Numbers of executors (per node)
            config.put("spoutExecutors", "1");
            config.put("mapExecutors", map);
            config.put("reduceExecutors", reduce);

            //input output relative to the storage directory
            config.putIfAbsent("storage", "./");
            config.put("input", input);
            config.put("output", output);

            logger.info("************ Creating the job request ***************");

            FileSpout spout = new WordFileSpout();
            MapBolt bolt = new MapBolt();
            ReduceBolt bolt2 = new ReduceBolt();
            PrintBolt printer = new PrintBolt();

            TopologyBuilder builder = new TopologyBuilder();

            // Only one source ("spout") for the words
            builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));

            // Parallel mappers, each of which gets specific words
            builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));

            // Parallel reducers, each of which gets specific words
            builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));

            // Only use the first printer bolt for reducing to a single point
            builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);

            Topology topo = builder.createTopology();

            WorkerJob job = new WorkerJob(topo, config);

            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
            try {
                String[] workers = WorkerHelper.getWorkers(config);

                int i = 0;
                for (String dest: workers) {
                    config.put("workerIndex", String.valueOf(i++));
                    if (sendJob(dest, "POST", config, "definejob",
                            mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
                            HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }
                }
                for (String dest: workers) {
                    if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() !=
                            HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job execution request failed");
                    }
                }
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error("jsonProcessing Error");
            }
            response.status(200);
            logger.info("submit work successfully");
            response.redirect("/status");
            return "submit work successfully";
        });


    }

    static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
        URL url = new URL(dest + "/" + job);

        logger.info("Sending request to " + url.toString());

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);

        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
        } else
            conn.getOutputStream();

        return conn;
    }


    public static void shutdown() {
        get("/shutdown", (request, response) -> {
            System.out.println("Press [Enter] to shut down this node...");
            try {
                (new BufferedReader(new InputStreamReader(System.in))).readLine();
            } catch (IOException e) {
                logger.info("shutdown readline error");
            }

            WorkerServer.shutdown();
            System.exit(0);
            return "shutdown";
        });

    }

    /**
     * The mainline for launching a MapReduce Master.  This should
     * handle at least the status and workerstatus routes, and optionally
     * initialize a worker as well.
     * 
     * @param args
     */


    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn", Level.DEBUG);

        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        port(myPort);

        System.out.println("Master node startup, on port " + myPort);

        // TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce here
        registerStatusPage();

        // TODO: route handler for /workerstatus reports from the workers
        workerReport();

        launch();

        shutdown();
    }
}

