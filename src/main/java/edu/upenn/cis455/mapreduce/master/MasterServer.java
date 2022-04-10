package edu.upenn.cis455.mapreduce.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.*;

public class MasterServer {
    private static final Logger logger = LogManager.getLogger(MasterServer.class);
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
            singleWorkerInfo.put("jobs",job);
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
                    "<body>Hi, I am the master!</body></html>";
            StringBuilder res = new StringBuilder(head);

            int n = 0;
            for(Map<String, String> singleWorker: workersMap.values()){
                String id = n + ": ";
                String port = "port=" + singleWorker.get("port");
                String status = "status=" + singleWorker.get("status");
                String job = "job=" + singleWorker.get("job");
                String keysRead = "keysRead=" + singleWorker.get("keysRead");
                String keysWritten = "keysWritten=" + singleWorker.get("kesingleWorkerysWritten");
                String results = "results=" + singleWorker.get("results");
                String info = String.join(", ", port, status, job, keysRead, keysWritten, results);
                res.append(id).append(info).append("<br>");

            }


            String form = "<form method=\"POST\" action=\"/submitjob\">\n" +
                    "    Job Name: <input type=\"text\" name=\"jobname\"/><br/>\n" +
                    "    Class Name: <input type=\"text\" name=\"classname\"/><br/>\n" +
                    "    Input Directory: <input type=\"text\" name=\"input\"/><br/>\n" +
                    "    Output Directory: <input type=\"text\" name=\"output\"/><br/>\n" +
                    "    Map Threads: <input type=\"text\" name=\"map\"/><br/>\n" +
                    "    Reduce Threads: <input type=\"text\" name=\"reduce\"/><br/>\n" +
                    "</form>";

            res.append(form);
            return res.toString();

        });

    }


    public static void shutdown() {

    }

    /**
     * The mainline for launching a MapReduce Master.  This should
     * handle at least the status and workerstatus routes, and optionally
     * initialize a worker as well.
     * 
     * @param args
     */


    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        port(myPort);

        System.out.println("Master node startup, on port " + myPort);

        // TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce here
        registerStatusPage();
        workerReport();

        // TODO: route handler for /workerstatus reports from the workers
    }
}

