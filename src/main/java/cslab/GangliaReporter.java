package cslab;

import static com.yahoo.ycsb.Client.gangserver;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author cmantas
 */
public class GangliaReporter {
    
    //a mapping from metric_name-->metric_units
    private  static HashMap<String, String> metrics = new HashMap(5);
    
    private static String[] commandString(String name, double value, String units){
        String[] command;
        if (gangserver != null)
            /*command = new String[]{"gmetric", "-n", name, 
                 "-v", Double.toString(value), "-d", "15", "-t", "float", "-u",
                 "reqs/sec", "-S", gangserver
        */
             command = new String[]{"gmetric", "-n", name, 
                 "-v", Double.toString(value),  "-t", "float", "-u",
                 "reqs/sec", "-S", gangserver};
        else 
            command =  new String[]{"gmetric", "-n", name,
                "-v", Double.toString(value),  "-t", "float", "-u", 
                units};
        return command;
    }
    
    private static void executeCommand(String[] command){
        try {
            Process child = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void report(String name, double value, String units, boolean verbose) {
        String[] ganglia_command;
        ganglia_command = commandString(name, value, units);
        if (verbose) System.err.println("\tGANG: reporting " + name + "=" + value + units);
        executeCommand(ganglia_command);
        synchronized(metrics){
            metrics.put(name, units);
        }
    }
    
    public static void report(String name, String ClientNo, double value, String units) {
        String postfix = ClientNo!=null ? "_"+ClientNo : "";
        report(name + postfix, value, units, false);
    }
    
    public static void reportAllZero(){
        synchronized(metrics){
        for(String name:metrics.keySet()){
            System.err.println("\tGANG: reporting EOL for "+name);
            report(name, 0, metrics.get(name), false);
        }
    }
    }

}
