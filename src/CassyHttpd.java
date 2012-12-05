/**
 * Created with IntelliJ IDEA.
 * User: mkongtongk
 * Date: 29/11/12
 * Time: 16:00
 * To change this template use File | Settings | File Templates.
 */

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class CassyHttpd {
    private static final String DEFAULT_HOST = "192.168.56.102";
    private static final int DEFAULT_PORT = 7199;

    private NodeProbe probe;
    //private static final Logger logger = LoggerFactory.getLogger(CassyCmd.class);

    public CassyHttpd(NodeProbe probe)
    {
        this.probe = probe;
    }

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     *
     * @throws IOException on connection failures
     */
    public void start() throws IOException {

        HttpServer httpServer = HttpServer.create();

        httpServer.createContext("/nodetool", new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                httpExchange.getResponseHeaders().put("Content-Type", Collections.singletonList("text/plain; charset=UTF8"));
                StringBuilder body = new StringBuilder();

                append(body,"", printRing(null) );
                append(body,"", printThreadPoolStats());
                append(body,"", printColumnFamilyStats());

                OutputStream out = httpExchange.getResponseBody();

                byte[] bytes = body.toString().getBytes("UTF8");
                httpExchange.sendResponseHeaders(200, bytes.length);
                out.write(bytes);
                httpExchange.close();
            }


            public String printRing(String keyspace)
            {
                StringBuilder body = new StringBuilder();
                Map<String, String> tokenToEndpoint = probe.getTokenToEndpointMap();
                List<String> sortedTokens = new ArrayList<String>(tokenToEndpoint.keySet());

                Collection<String> liveNodes = probe.getLiveNodes();
                Collection<String> deadNodes = probe.getUnreachableNodes();
                Collection<String> joiningNodes = probe.getJoiningNodes();
                Collection<String> leavingNodes = probe.getLeavingNodes();
                Collection<String> movingNodes = probe.getMovingNodes();
                Map<String, String> loadMap = probe.getLoadMap();

                String format = "%-16s%-12s%-12s%-7s%-8s%-16s%-20s%-44s%n";

                // Calculate per-token ownership of the ring
                Map<String, Float> ownerships;
                try
                {
                    ownerships = probe.effectiveOwnership(keyspace);
                    body.append(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Effective-Ownership", "Token"));
                }
                catch (ConfigurationException ex)
                {
                    ownerships = probe.getOwnership();
                    body.append(String.format("Note: Ownership information does not include topology, please specify a keyspace. \n"));
                    body.append(String.format(format, "Address", "DC", "Rack", "Status", "State", "Load", "Owns", "Token"));
                }

                // show pre-wrap token twice so you can always read a node's range as
                // (previous line token, current line token]
                if (sortedTokens.size() > 1)
                    body.append(String.format(format, "", "", "", "", "", "", "", sortedTokens.get(sortedTokens.size() - 1)));

                for (String token : sortedTokens)
                {
                    String primaryEndpoint = tokenToEndpoint.get(token);
                    String dataCenter;
                    try
                    {
                        dataCenter = probe.getEndpointSnitchInfoProxy().getDatacenter(primaryEndpoint);
                    }
                    catch (UnknownHostException e)
                    {
                        dataCenter = "Unknown";
                    }
                    String rack;
                    try
                    {
                        rack = probe.getEndpointSnitchInfoProxy().getRack(primaryEndpoint);
                    }
                    catch (UnknownHostException e)
                    {
                        rack = "Unknown";
                    }
                    String status = liveNodes.contains(primaryEndpoint)
                            ? "Up"
                            : deadNodes.contains(primaryEndpoint)
                            ? "Down"
                            : "?";

                    String state = "Normal";

                    if (joiningNodes.contains(primaryEndpoint))
                        state = "Joining";
                    else if (leavingNodes.contains(primaryEndpoint))
                        state = "Leaving";
                    else if (movingNodes.contains(primaryEndpoint))
                        state = "Moving";

                    String load = loadMap.containsKey(primaryEndpoint)
                            ? loadMap.get(primaryEndpoint)
                            : "?";
                    String owns = new DecimalFormat("##0.00%").format(ownerships.get(token) == null ? 0.0F : ownerships.get(token));
                    body.append(String.format(format, primaryEndpoint, dataCenter, rack, status, state, load, owns, token));
                }
                body.append(String.format("%n"));
                return body.toString();
            }

            public String printThreadPoolStats()
            {
                StringBuilder body = new StringBuilder();
                body.append(String.format("%-25s%10s%10s%15s%10s%18s%n", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked"));

                Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
                while (threads.hasNext())
                {
                    Map.Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
                    String poolName = thread.getKey();
                    JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
                    body.append(String.format("%-25s%10s%10s%15s%10s%18s%n",
                            poolName,
                            threadPoolProxy.getActiveCount(),
                            threadPoolProxy.getPendingTasks(),
                            threadPoolProxy.getCompletedTasks(),
                            threadPoolProxy.getCurrentlyBlockedTasks(),
                            threadPoolProxy.getTotalBlockedTasks()));
                }
                body.append(String.format("%n"));
                return body.toString();
            }

            public String printColumnFamilyStats()
            {
                StringBuilder body = new StringBuilder();
                Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

                // get a list of column family stores
                Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe.getColumnFamilyStoreMBeanProxies();

                while (cfamilies.hasNext())
                {
                    Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
                    String tableName = entry.getKey();
                    ColumnFamilyStoreMBean cfsProxy = entry.getValue();

                    if (!cfstoreMap.containsKey(tableName))
                    {
                        List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                        columnFamilies.add(cfsProxy);
                        cfstoreMap.put(tableName, columnFamilies);
                    }
                    else
                    {
                        cfstoreMap.get(tableName).add(cfsProxy);
                    }
                }

                // print out the table statistics
                for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
                {
                    String tableName = entry.getKey();
                    List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
                    long tableReadCount = 0;
                    long tableWriteCount = 0;
                    int tablePendingTasks = 0;
                    double tableTotalReadTime = 0.0f;
                    double tableTotalWriteTime = 0.0f;

                    body.append("Keyspace: " + tableName + "\n");
                    for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                    {
                        long writeCount = cfstore.getWriteCount();
                        long readCount = cfstore.getReadCount();

                        if (readCount > 0)
                        {
                            tableReadCount += readCount;
                            tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                        }
                        if (writeCount > 0)
                        {
                            tableWriteCount += writeCount;
                            tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                        }
                        tablePendingTasks += cfstore.getPendingTasks();
                    }

                    double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
                    double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

                    body.append("\tRead Count: " + tableReadCount + "\n");
                    body.append("\tRead Latency: " + String.format("%s", tableReadLatency) + " ms." + "\n");
                    body.append("\tWrite Count: " + tableWriteCount + "\n");
                    body.append("\tWrite Latency: " + String.format("%s", tableWriteLatency) + " ms." + "\n");
                    body.append("\tPending Tasks: " + tablePendingTasks + "\n");

                    // print out column family statistics for this table
                    for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                    {
                        body.append("\t\tColumn Family: " + cfstore.getColumnFamilyName() + "\n");
                        body.append("\t\tSSTable count: " + cfstore.getLiveSSTableCount() + "\n");
                        body.append("\t\tSpace used (live): " + cfstore.getLiveDiskSpaceUsed() + "\n");
                        body.append("\t\tSpace used (total): " + cfstore.getTotalDiskSpaceUsed() + "\n");
                        body.append("\t\tNumber of Keys (estimate): " + cfstore.estimateKeys() + "\n");
                        body.append("\t\tMemtable Columns Count: " + cfstore.getMemtableColumnsCount() + "\n");
                        body.append("\t\tMemtable Data Size: " + cfstore.getMemtableDataSize() + "\n");
                        body.append("\t\tMemtable Switch Count: " + cfstore.getMemtableSwitchCount() + "\n");
                        body.append("\t\tRead Count: " + cfstore.getReadCount() + "\n");
                        body.append("\t\tRead Latency: " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000) + " ms." + "\n");
                        body.append("\t\tWrite Count: " + cfstore.getWriteCount() + "\n");
                        body.append("\t\tWrite Latency: " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000) + " ms." + "\n");
                        body.append("\t\tPending Tasks: " + cfstore.getPendingTasks() + "\n");
                        body.append("\t\tBloom Filter False Postives: " + cfstore.getBloomFilterFalsePositives() + "\n");
                        body.append("\t\tBloom Filter False Ratio: " + String.format("%01.5f", cfstore.getRecentBloomFilterFalseRatio()) + "\n");
                        body.append("\t\tBloom Filter Space Used: " + cfstore.getBloomFilterDiskSpaceUsed() + "\n");
                        body.append("\t\tCompacted row minimum size: " + cfstore.getMinRowSize() + "\n");
                        body.append("\t\tCompacted row maximum size: " + cfstore.getMaxRowSize() + "\n");
                        body.append("\t\tCompacted row mean size: " + cfstore.getMeanRowSize() + "\n");

                        body.append("" + "\n");
                    }
                    body.append("----------------" + "\n");
                }
                return body.toString();
            }

            private void append(StringBuilder body, String name, Object value) {
                body.append(name).append('=').append(value).append('\n');
            }
        });
        httpServer.bind(new InetSocketAddress(9090), 0);
        httpServer.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        String host = args.length > 0 ? args[0] : "192.168.56.102";
//        CassyHttpd server = new CassyHttpd(host);

        String username = "";
        String password = "";

        NodeProbe probe = null;
        try
        {
            probe = username == null ? new NodeProbe(DEFAULT_HOST, DEFAULT_PORT) : new NodeProbe(DEFAULT_HOST, DEFAULT_PORT, username, password);
        }
        catch (IOException ioe)
        {
            System.err.printf("Cannot resolve '%s': unknown host\n", new Object[] { DEFAULT_HOST });
            System.exit(1);
        }
        CassyHttpd cassyHttpd = new CassyHttpd(probe);
        cassyHttpd.start();
    }
}
