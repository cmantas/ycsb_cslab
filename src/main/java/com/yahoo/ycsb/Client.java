/*
 * ==> edit for ganglia reporting from CSLab, NTUA Tiramola team
   = added ganglia reporting, sinusoidal load, changed Vector to ArrrayList
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;


import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import static cslab.GangliaReporter.*;
import java.io.*;
import java.text.DecimalFormat;
import java.util.*;


//import org.apache.log4j.BasicConfigurator;

/**
 * A thread to periodically show the status of the experiment, to reassure you that progress is being made.
 * 
 * @author cooperb
 *
 */
class StatusThread extends Thread
{
	ArrayList<Thread> _threads;
	String _label;
	boolean _standardstatus;
	Properties _props;
	int _sinusoidal_average, _sinusoidal_offset;
	private boolean _is_sinusoidal;
	private double _sinusoidal_period;
	private int count=0;
	/**
	 * The interval for reporting status.
	 */

	/*dtrihinas-ikons*/
	//public static final long sleeptime=10000;
	public static long sleeptime;
    /**/

	public StatusThread(ArrayList<Thread> threads, String label, boolean standardstatus, Properties props)
	{
		_threads=threads;
		_label=label;
		_standardstatus=standardstatus;
		_props=props;
		_sinusoidal_offset=Integer.parseInt(_props.getProperty(Client.SINUSOIDAL_OFFSET,"500"));
		_sinusoidal_average=Integer.parseInt(_props.getProperty("target","0"));
		_is_sinusoidal=Boolean.parseBoolean(_props.getProperty(Client.IS_SINUSOIDAL_PROPERTY, "false"));
		_sinusoidal_period=Double.parseDouble(_props.getProperty(Client.SINUSOIDAL_PERIOD,"10"));
	}

	/**
	 * Run and periodically report status.
	 */
	public void run()
	{
		long st=System.currentTimeMillis();

		long lasten=st;
		long lasttotalops=0;
		Random ran = new Random();
		boolean alldone;
		double prevlamda=_sinusoidal_average;
                //DEBUG: keep previous latency
		int prev_latency=0;
                
		do 
		{
			alldone=true;
			long synced_time = System.currentTimeMillis();

			int totalops=0;
			int totalopscreated=0;
			//terminate this thread when all the worker threads are done
			for (Thread t : _threads)
			{
				if (t.getState()!=Thread.State.TERMINATED)
				{
					alldone=false;
				}

				ClientThread ct=(ClientThread)t;
				//the following sets the synced_time to all threads, so that the 
				//function that calculates the frequency as a sinusoidal function of time
				//has the same time for every thread.
				
				ct.setSynced_time(synced_time);
				totalops+=ct.getOpsDone();
				totalopscreated += ct._opscreated;
				
				/*dtrihinas-ikons*/
				sleeptime = 5000;//Client.report_period;
				/**/
			}

			long en=System.currentTimeMillis();

			long interval=en-st;
			//double throughput=1000.0*((double)totalops)/((double)interval);
			// SINUSOIDAL START
			double curthroughput=1000.0*(((double)(totalops-lasttotalops))/((double)(en-lasten)));			       
			
			
			double curlamda=_sinusoidal_average;
			//update lambda
	                   if (_is_sinusoidal) {
                        double time = ((double) (System.currentTimeMillis() - st)) / ((double) (_sinusoidal_period * 1000));
                        curlamda = _sinusoidal_average + _sinusoidal_offset * Math.sin((2 * Math.PI * time + 3 * Math.PI / 2));
                        ClientThread.curtargetperthread = curlamda / _threads.size();
                        //System.out.println(ClientThread.curtargetperthread);
                        File f = new File(Client.hostsFile + "_t");
                        if (!f.exists()) {
                            BufferedReader br = null;
                            List<String> hostst = new ArrayList<String>();
                            try {
                                br = new BufferedReader(new FileReader(Client.hostsFile));
                                String line = br.readLine();
                                hostst.add(line);
                                while (line != null) {
                                    hostst.add(line);
                                    line = br.readLine();
                                }
                            } catch (Exception e1) {
                                hostst.add("localhost");
                            } finally {
                                try {
                                    br.close();
                                } catch (Exception e) {
                                }
                            }
                            if (!hostst.isEmpty()) {
                                if (!Client.hosts.containsAll(hostst)) {//new host
                                    hostst.removeAll(Client.hosts);
//                                    if (hostst.isEmpty()) {
                                        for (String h : hostst) {
                                            System.out.println(h);
                                        }
                                        System.out.println("Hosts added: " + hostst);
                                        for (String host : hostst) {
                                            int k = _threads.size() / Client.hosts.size();
                                            for (int i = 0; i < k; i++) {
                                                ClientThread t = (ClientThread) _threads.get(ran.nextInt(_threads.size()));
                                                t.nextHost = host;
                                                t.restart = true;
                                                System.out.println("restarting: " + t.nextHost);
                                            }
                                        }
                                        Client.hosts.addAll(hostst);
//                                    }
                                } else {
                                    Client.hosts.removeAll(hostst);
                                    if (!Client.hosts.isEmpty()) {//removed host
                                        System.out.println("Hosts removed: " + Client.hosts);
                                        for (Thread ct : _threads) {
                                            System.out.println("Checking: " + ((ClientThread) ct).nextHost);
                                            if (Client.hosts.contains(((ClientThread) ct).nextHost)) {
                                                ((ClientThread) ct).nextHost = hostst.get(ran.nextInt(hostst.size()));
                                                ((ClientThread) ct).restart = true;
                                                System.out.println("restarting: " + ((ClientThread) ct).nextHost);
                                            }
                                        }
                                    }
                                    Client.hosts = hostst;
                                }

                            }
                        }
                    }
			/*if(count>20){
				int i=0;
				for(Thread ct : _threads){
					((ClientThread)ct).nextHost=Client.hosts.get(i%Client.hosts.size());
					((ClientThread)ct).restart=true;
					i++;
				}
				count=0;
			}*/
			
			lasttotalops=totalops;
			lasten=en;
			
			DecimalFormat d = new DecimalFormat("#");
						
			
                    /*dtrihinas - ikons*/
                    //report lamda to local JCatascopia Monitoring Agent via XProbe
                    if (Client.xprobe != null) {
                        String[] xprobeCMD = new String[]{"java", "-jar", Client.xprobe, "--name:ycsb_target", "--units:req/sec", "--type:DOUBLE", "--value:" + d.format(prevlamda), "--group:ycsb_client_" + Client.clientno};

                        try {
                            Process proc = Runtime.getRuntime().exec(xprobeCMD);
                            if (Client.debug_mode) {
                                BufferedReader b = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                                String line;
                                while ((line = b.readLine()) != null) {
                                    System.err.println(line);
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }

	                   
                        //report ycsb_TARGET
                        report ("ycsb_TARGET", Client.clientno, prevlamda,"req/sec");
				
			
			
			if (totalops==0)
			{
				/*dtrihinas - ikons*/
				//report throughput to local JCatascopia Monitoring Agent via XProbe
                            {//BEGIN report throughput to local JCatascopia
				if (Client.xprobe != null){
					String[] xprobeCMD = new String[]{"java", "-jar", Client.xprobe, "--name:ycsb_throughput", "--units:req/sec", "--type:DOUBLE", "--value:0",  "--group:ycsb_client_"+Client.clientno};
					try {
						Process proc = Runtime.getRuntime().exec(xprobeCMD);
						if (Client.debug_mode){
				            BufferedReader b = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
							String line = "";
							while ((line = b.readLine()) != null){
							  System.err.println(line);
							}
						}
					} catch (IOException e){e.printStackTrace();}
                                        catch (Exception e){e.printStackTrace();}				
	    		}
                        }//END report throughput to local JCatascopia
				
                                //print to stderr
				System.err.println(_label+" "+(interval/1000)+" sec: " + " lamda: " + d.format(curlamda) + Measurements.getMeasurements().getSummary());
				
                                //report zero to ganglia
                                {//BEGIN ganlia report
                                report("ycsb_THROUGHPUT", Client.clientno, 0, "reqs/sec");
                                report("ycsb_READ",Client.clientno, 0, "msec");
                                report("ycsb_UPDATE", Client.clientno, 0, "reqs/sec");
                                Measurements.getMeasurements().getSummary();
                                }//END ganglia report
			}//if totalops=0
			else
			{
				
				//getMeasurments also reports to ganglia
				String m = Measurements.getMeasurements().getSummary();
				System.err.println(_label+" "+(interval/1000)+" sec: "+d.format(curthroughput)+" ops/sec; " + "  lambda " + d.format(prevlamda)  +"  "+m);
				prevlamda=curlamda;
				String[] latstring = m.split("=");
				/*for (int i = 0; i < latstring.length; i++) {
					System.out.println(latstring[i]+"   !!");
				}*/
				double latency=0;
				if(latstring.length>=2){
					latency=Double.parseDouble(latstring[1].substring(0,latstring[1].indexOf("]")));
				}
                                
                                //DEBUG: hardcoded latency creates problems, why is that needed???
				if(latency>=1000000) latency=1000000;
				
                                
				/*dtrihinas - ikons*/
				//report throughput to local JCatascopia Monitoring Agent via XProbe
                                {//BEGIN JCAtascopia
                                if (Client.xprobe != null) {
                                    String[] xprobeCMD = new String[]{"java", "-jar", Client.xprobe, "--name:ycsb_throughput", "--units:req/sec", "--type:DOUBLE", "--value:" + d.format(curthroughput), "--group:ycsb_client_" + Client.clientno};
                                    try {
                                        Process proc = Runtime.getRuntime().exec(xprobeCMD);
                                        if (Client.debug_mode) {
                                            BufferedReader b = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                                            String line = "";
                                            while ((line = b.readLine()) != null) {
                                                System.err.println(line);
                                            }
                                        }
                                    } catch (IOException e) {

                                        e.printStackTrace();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }

                                    //System.out.println("Latency: "+ latency+" !!!");
                                    xprobeCMD = new String[]{"java", "-jar", Client.xprobe, "--name:ycsb_latency", "--units:us", "--type:DOUBLE", "--value:" + d.format(latency), "--group:ycsb_client_" + Client.clientno};

                                    try {
                                        Process proc = Runtime.getRuntime().exec(xprobeCMD);
                                        if (Client.debug_mode) {
                                            BufferedReader b = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                                            String line = "";
                                            while ((line = b.readLine()) != null) {
                                                System.err.println(line);
                                            }
                                        }
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                }//END JCAtascopia
                                
                                //report troughput to gagnlia
                                report("ycsb_THROUGHPUT", Client.clientno, curthroughput, "reqs/sec");
                                //getSummary will also report to gaglia
                                Measurements.getMeasurements().getSummary();



			}
			
			count++;
			//System.out.println("_standardstatus: " + _standardstatus);
			if (_standardstatus)
			{
			if (totalops==0)
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+Measurements.getMeasurements().getSummary());
			}
			else
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
			}
			}

			try
			{
				sleep(sleeptime);
			}
			catch (InterruptedException e)
			{
				//do nothing
			}

		}
		while (!alldone);
	}
}

/**
 * A thread for executing transactions or data inserts to the database.
 * 
 * @author cooperb
 *
 */
class ClientThread extends Thread
{
	static Random random=new Random();
	
	public static double curtargetperthread;
	public boolean restart;
	public String nextHost;
	
	DB _db;
	boolean _dotransactions;
	Workload _workload;
	int _opcount;
	double _target;

	int _opsdone;
	int _opscreated;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;
	boolean _is_sinusoidal;
	double _sinusoidal_period;
	double _max_target;
	//updated by the statusThread every 10 seconds.
	//contains the current time, and is used to calculate the 
	//current frequency as a sinusoidal function of time.
	// the synced time is the same for every thread.
	long _synced_time;
	
	
	public void setSynced_time(long synced_time) {
		this._synced_time = synced_time;
	}

	/**
	 * Constructor.
	 * 
	 * @param db the DB implementation to use
	 * @param dotransactions true to do transactions, false to insert data
	 * @param workload the workload to use
	 * @param threadid the id of this thread 
	 * @param threadcount the total number of threads 
	 * @param props the properties defining the experiment
	 * @param opcount the number of operations (transactions or inserts) to do
	 * @param targetperthreadperms target number of operations per thread per ms
	 */
	public ClientThread(DB db, boolean dotransactions, Workload workload, int threadid, int threadcount, Properties props, int opcount, double targetperthreadperms, long synced_time)
	{
		//TODO: consider removing threadcount and threadid
		_db=db;
		_dotransactions=dotransactions;
		_workload=workload;
		_opcount=opcount;
		_opsdone=0;
		_opscreated=0;
		_target=targetperthreadperms;
		_max_target=targetperthreadperms;
		_threadid=threadid;
		_threadcount=threadcount;
		_props=props;
		//System.out.println("Interval = "+interval);
		_is_sinusoidal=Boolean.parseBoolean(_props.getProperty(Client.IS_SINUSOIDAL_PROPERTY, "false"));
		//System.out.println("period: + " + props.getProperty(Client.SINUSOIDAL_PERIOD));
		_sinusoidal_period=Double.parseDouble(_props.getProperty(Client.SINUSOIDAL_PERIOD,"10"));
		_synced_time=synced_time;
		restart=false;
	}



	public int getOpsDone()
	{
		return _opsdone;
	}

	public void run()
	{
		try
		{
			_db.init();
		}
		catch (DBException e)
		{       
                        System.err.println("DB init failed");
			e.printStackTrace();
			return;
		}

		try
		{
			_workloadstate=_workload.initThread(_props,_threadid,_threadcount);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}

		//spread the thread operations out so they don't all hit the DB at the same time
//		try
//		{
//		   //GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
//		   //and the sleep() doesn't make sense for granularities < 1 ms anyway
//			//check if the sinusoidal property is enabled. if so, then the _target depends on the
//			//current time, according to the sinus period.
//
//
//			//check if the sinusoidal property is enabled. if so, then the _target depends on the
//			//current time, according to the sinus period.
//			if(_is_sinusoidal){
//				//_target=_max_target*Math.sin((2*Math.PI*_synced_time*0.001)/_sinusoidal_period) +_max_target;
//				if(_threadid==1){
//					//System.out.println(" maxTarget " + _max_target  +  " currtarget: " + _target );					
//				}
//			}
//
//			
//
//			if ( (_target>0) && (_target<=1.0) ) 
//		   {
//			  sleep(random.nextInt((int)(1.0/_target)));
//		   }
//		}
//		catch (InterruptedException e)
//		{
//		   //do nothing
//		}
		
		try
		{
			int opsdone1=0;
			if (_dotransactions)
			{
				long st=System.nanoTime();
				nextHost=_props.get("hosts").toString();
				System.err.println("Hosts ["+this.getName()+"]: "+_props.get("hosts"));
				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				{

					long t=System.nanoTime();
					if(t-st>=new Long("1000000000")){//refresh timers and target
						opsdone1=0;
						st=System.nanoTime();
					}
					/*if(t-st>=new Long("5000000000")){
						File f = new File(Client.hostsFile+"_t");
						if(!f.exists()) { 
							boolean live=false;
							BufferedReader br=null;
							List<String> hosts= new ArrayList<String>();
							try {
								br = new BufferedReader(new FileReader(Client.hostsFile));
						        String line = br.readLine();
						        hosts.add(line);
						        while (line != null) {
						        	if(line.equals(_props.get("hosts"))){
						        		live=true;
						        		break;
						        	}
							        hosts.add(line);
						            line = br.readLine();
						        }
							} catch (Exception e1) {
								e1.printStackTrace();
						    } finally {
						        try {
									br.close();
								} catch (IOException e) {
									e.printStackTrace();
								}
						    }
							if(!live){//restart db with other host
								try
								{
									_db.cleanup();
								}
								catch (DBException e)
								{
									e.printStackTrace();
									e.printStackTrace(System.out);
									return;
								}
								_db=null;
								Random ran = new Random();
								String dbname=_props.getProperty("db","com.yahoo.ycsb.BasicDB");
								try
								{
									_props.remove("hosts");
									System.out.println("New hosts:"+hosts);
									_props.setProperty("hosts", hosts.get(ran.nextInt(hosts.size())));
									System.out.println("New host:"+_props.get("hosts"));
									_db=DBFactory.newDB(dbname,_props);
								}
								catch (UnknownDBException e)
								{
									System.out.println("Unknown DB "+dbname);
									System.exit(0);
								}
								try
								{
									_db.init();
								}
								catch (DBException e)
								{
									e.printStackTrace();
									e.printStackTrace(System.out);
									return;
								}
	
								try
								{
									_workloadstate=_workload.initThread(_props,_threadid,_threadcount);
								}
								catch (WorkloadException e)
								{
									e.printStackTrace();
									e.printStackTrace(System.out);
									return;
								}
								st=System.nanoTime();
								opsdone1=0;
							}
						}
					}*/
					if(restart){
						try
						{
							_db.cleanup();
						}
						catch (DBException e)
						{
							e.printStackTrace();
							e.printStackTrace(System.out);
							return;
						}
						_db=null;
						String dbname=_props.getProperty("db","com.yahoo.ycsb.BasicDB");
						try
						{
							_props.remove("hosts");
							_props.setProperty("hosts", nextHost);
							System.out.println("New host:"+_props.get("hosts"));
							_db=DBFactory.newDB(dbname,_props);
						}
						catch (UnknownDBException e)
						{
							System.out.println("Unknown DB "+dbname);
							System.exit(0);
						}
						try
						{
							_db.init();
						}
						catch (DBException e)
						{
							e.printStackTrace();
							e.printStackTrace(System.out);
							return;
						}

						try
						{
							_workloadstate=_workload.initThread(_props,_threadid,_threadcount);
						}
						catch (WorkloadException e)
						{
							e.printStackTrace();
							e.printStackTrace(System.out);
							return;
						}
						st=System.nanoTime();
						opsdone1=0;
						restart=false;
					}
					
					if (!_workload.doTransaction(_db,_workloadstate))
					{
						break;
					}

					_opsdone++;
					opsdone1++;
					//throttle the operations
					
					if (curtargetperthread>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						//long timeleft=Math.round((((double)opsdone1*1000.0)/(double)curtargetperthread))-(System.currentTimeMillis()-st);
						//timeleft/=10;
						while (((System.nanoTime()-st)/1000000000.0)<(((double)opsdone1)/curtargetperthread))
						{
							try
							{
								Thread.sleep(0, 100000);
								//sleep(1);
								//timeleft=Math.round((((double)opsdone1*1000.0)/(double)curtargetperthread))-(System.currentTimeMillis()-st);
								//timeleft/=10;
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}

						}
					}
				}
			}
			else
			{
				long st=System.currentTimeMillis();

				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				{

					if (!_workload.doInsert(_db,_workloadstate))
					{
						break;
					}

					_opsdone++;

					//throttle the operations
					if (_target>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						while (System.currentTimeMillis()-st<((double)_opsdone)/_target)
						{
							try 
							{
								sleep(1);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			_db.cleanup();
		}
		catch (DBException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}
	}
	
	
	static double expon(double lamda) {
		double rnd; // random number (0 < rnd < 1)

		// Pull a uniform random number (0 < z < 1)
		do {
			rnd = Math.random();
		} while ((rnd == 0.0) || (rnd == 1.0));

		// Compute exponential random variable using inversion method
		return (-lamda * Math.log(rnd));
	}
	
	
}

/**
 * Main class for executing YCSB.
 */
public class Client
{
    
        //ganglia related vars
    public static String gangserver;
    public static String clientno;

	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";
	
	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";
        /* The maximum amount of time (in seconds) for which the benchmark will be run. */
        public static final String MAX_EXECUTION_TIME = "maxexecutiontime";
	/**
	 * Indicates whether the workload has a sinusoidal pattern,
	 * i.e., it increases and decreases its maximum throughput according 
	 * to a sinus function
	 */
	public static final String IS_SINUSOIDAL_PROPERTY="sinusoidal";
	/**
	 * In case the sinusoidal property is set to true, this parameter sets 
	 * the sinus period in seconds.
	 */
	public static final String SINUSOIDAL_PERIOD="period";
	public static final String SINUSOIDAL_OFFSET="offset";
	
	/*dtrihinas - ikons*/
	public static String xprobe;
	public static boolean debug_mode;
	public static long report_period;
	/**/
	
	//indicates whether the client should report or not the latency/throughput/lamda to a ganglia server.
	// throttled by the gangserver command line parameter, given in the form: serverip:serverhostname
	
	
	public static void usageMessage()
	{
		System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
				"              \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
				"             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
				"              can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("                   be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out.println("  "+WORKLOAD_PROPERTY+": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
		System.out.println("");
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
	}

	public static boolean checkRequiredProperties(Properties props)
	{
		if (props.getProperty(WORKLOAD_PROPERTY)==null)
		{
			System.out.println("Missing property: "+WORKLOAD_PROPERTY);
			return false;
		}

		return true;
	}


	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime)
			throws IOException
	{
		MeasurementsExporter exporter = null;
		try
		{
			// if no destination file is provided the results will be written to stdout
			OutputStream out;
			String exportFile = props.getProperty("exportfile");
			if (exportFile == null)
			{
				out = System.out;
			} else
			{
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try
			{
				exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e)
			{
				System.err.println("Could not find exporter " + exporterStr
						+ ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally
		{
			if (exporter != null)
			{
				exporter.close();
			}
		}
	}
	
	public static List<String> hosts;
	public static String hostsFile;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		String dbname;
		Properties props=new Properties();
		Properties fileprops=new Properties();
		boolean dotransactions=true;
		int threadcount=1;
		int target=0;
		boolean status=false;
		String label="";

		//parse arguments
		int argindex=0;

		if (args.length==0)
		{
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-"))
		{
			if (args[argindex].compareTo("-threads")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int tcount=Integer.parseInt(args[argindex]);
				props.setProperty("threadcount", tcount+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-target")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int ttarget=Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-load")==0)
			{
				dotransactions=false;
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				dotransactions=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-s")==0)
			{
				status=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-db")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db",args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-l")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				label=args[argindex];
				argindex++;
			}
			else if (args[argindex].compareTo("-P")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				String propfile=args[argindex];
				argindex++;

				Properties myfileprops=new Properties();
				try
				{
					myfileprops.load(new FileInputStream(propfile));
				}
				catch (IOException e)
				{
					System.out.println(e.getMessage());
					System.exit(0);
				}

				//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
				for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
				{
				   String prop=(String)e.nextElement();
				   
				   fileprops.setProperty(prop,myfileprops.getProperty(prop));
				}

			}
			else if (args[argindex].compareTo("-p")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int eq=args[argindex].indexOf('=');
				if (eq<0)
				{
					usageMessage();
					System.exit(0);
				}

				String name=args[argindex].substring(0,eq);
				String value=args[argindex].substring(eq+1);
				props.put(name,value);
				//System.out.println("["+name+"]=["+value+"]");
				argindex++;
			}
			else
			{
				System.out.println("Unknown option "+args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex>=args.length)
			{
				break;
			}
		}

		if (argindex!=args.length)
		{
			usageMessage();
			System.exit(0);
		}

		//set up logging
		//BasicConfigurator.configure();

		//overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
		   String prop=(String)e.nextElement();
		   
		   fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		if (!checkRequiredProperties(props))
		{
			System.exit(0);
		}
		
		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","1"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));
		
		/*dtrihinas - ikons*/
		xprobe = props.getProperty("xprobe");  
		//clientno = props.getProperty("clientno", Integer.toString(new Random().nextInt())); 
		clientno = props.getProperty("clientno");
		//ganglia reporting
                gangserver = props.getProperty("gangserver");	
                
		//compute the target throughput
		double targetperthreadperms=-1;
		if (target>0)
		{
			double targetperthread=((double)target)/((double)threadcount);
			target=Integer.parseInt(props.getProperty("target","0"));
			
			ClientThread.curtargetperthread=targetperthread;
			targetperthreadperms=targetperthread/1000.0;
		}	 

		System.out.println("YCSB Client 0.1 with ganglia reporting");
		System.out.print("Command line:");
		for (int i=0; i<args.length; i++)
		{
			System.out.print(" "+args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");
		
		//show a warning message that creating the workload is taking a while
		//but only do so if it is taking longer than 2 seconds 
		//(showing the message right away if the setup wasn't taking very long was confusing people)
		Thread warningthread=new Thread() 
		{
			public void run()
			{
				try
				{
					sleep(2000);
				}
				catch (InterruptedException e)
				{
					return;
				}
				System.err.println(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();
		
		//set up measurements
		Measurements.setProperties(props);
		
		//load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload=null;

		try 
		{
			Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

			workload=(Workload)workloadclass.newInstance();
		}
		catch (Exception e) 
		{  
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			workload.init(props);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		warningthread.interrupt();

		//run the workload

		System.err.println("Starting test.");

		int opcount;
		if (dotransactions)
		{
			opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
		}
		else
		{
			if (props.containsKey(INSERT_COUNT_PROPERTY))
			{
				opcount=Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY,"0"));
			}
			else
			{
				opcount=Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY,"0"));
			}
		}

		ArrayList<Thread> threads=new ArrayList<Thread>();

		long synced_time = System.currentTimeMillis();
		
		hosts = new ArrayList<String>();
		hostsFile = props.getProperty("hostsFile");
		BufferedReader br=null;
            try {
                br = new BufferedReader(new FileReader(hostsFile));
                String line = br.readLine();
                while (line != null) {
                    hosts.add(line);
                    line = br.readLine();
                }
            } catch (Exception e1) {
                //e1.printStackTrace();
                System.out.println("NO host file, using localhost");
                hosts.add("127.0.0.1");
	    } finally {
	        try {br.close();} catch (Exception e) {}
	    }
		System.out.println("Hosts: "+hosts);
		Random ran = new Random();
		List<String> suffleHosts = new ArrayList<String>();
		while(hosts.size()>0){
			String t =hosts.remove(ran.nextInt(hosts.size()));
			suffleHosts.add(t);
		}
		hosts=suffleHosts;
		int i=0;
		for (int threadid=0; threadid<threadcount; threadid++)
		{
			DB db=null;
			Properties p1 = new Properties(props);
			try
			{
				p1.setProperty("hosts", hosts.get(i%hosts.size()));
                                //workaround some problems with CQLClient
                                if (dbname.equals("com.yahoo.ycsb.db.CassandraCQLClient")){
                                    p1.setProperty("host", hosts.get(i%hosts.size()));
                                    p1.setProperty("port", "9042");
                                }
				db=DBFactory.newDB(dbname,p1);
			}
			catch (UnknownDBException e)
			{
				System.out.println("Unknown DB "+dbname);
				System.exit(0);
			}
			
			i++;
			Thread t=new ClientThread(db,dotransactions,workload,threadid,threadcount,p1,opcount/threadcount,targetperthreadperms,synced_time);

			threads.add(t);
			//t.start();
		}

		StatusThread statusthread=null;

		if (status)
		{
			boolean standardstatus=false;
			if (props.getProperty("measurementtype","").compareTo("timeseries")==0) 
			{
				standardstatus=true;
			}	
			statusthread=new StatusThread(threads,label,standardstatus,props);
			statusthread.start();
		}

		long st=System.currentTimeMillis();

		for (Thread t : threads)
		{
			t.start();
		}
		
    Thread terminator = null;
    
    if (maxExecutionTime > 0) {
      terminator = new TerminatorThread(maxExecutionTime, threads, workload);
      terminator.start();
    }
    
    int opsDone = 0;

		for (Thread t : threads)
		{
			try
			{
				t.join();
				opsDone += ((ClientThread)t).getOpsDone();
			}
			catch (InterruptedException e)
			{
			}
		}

		long en=System.currentTimeMillis();
		
		if (terminator != null && !terminator.isInterrupted()) {
      terminator.interrupt();
    }

		if (status)
		{
			statusthread.interrupt();
		}

		try
		{
			workload.cleanup();
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			exportMeasurements(props, opcount, en - st);
		} catch (IOException e)
		{
			System.err.println("Could not export measurements, error: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
                
                TextMeasurementsExporter e = new TextMeasurementsExporter(System.err);
               
                try {
                    Measurements.getMeasurements().exportMeasurements(e);
                } catch (IOException ex) {}
                
                //report zero value to ganglia for all metrics
                reportAllZero();
		System.exit(0);
	}
}
