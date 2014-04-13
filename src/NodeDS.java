import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;


public class NodeDS {
	private int nodeId;
	private int clientTimeStamp;
	private int timeStamp;
	private int maxClientsCount;
	private ServerSocket serverSocket = null;
	private Socket clientSocket = null;
	private Map<Integer,String[]> nodes = null;
	private Map<Integer,Integer> connectedNodes = null;
	private static final Object ndLock = new Object();
	private static final Object cnNdLock = new Object();
	private static final Object cnCodeLock = new Object();
	private static final Object tsCodeLock = new Object();
	private static final Object ndsLock = new Object();
	private static final Object tsLock = new Object();
	private static final Object mcLock = new Object();
	
	public NodeDS()
	{
		timeStamp = 0;
		nodes = new HashMap<Integer,String[]>();
		readConfig();
	}
	
	public Object getTsCodeLock()
	{
		return tsCodeLock;
	}
	
	public int getNodeId()
	{
		synchronized(ndLock)
		{
			return nodeId;
		}
	}
	
	public Map<Integer,Integer> getConnectedNodes()
	{
		synchronized(cnNdLock)
		{
			return connectedNodes;
		}
	}
	
	public Object getCnCodeLock()
	{
		return cnCodeLock;
	}
	
	
	public int getMaxClientsCount()
	{
		synchronized(ndLock)
		{
			return maxClientsCount;
		}
	}
	
	public void setMaxClientsCount(int _maxClientsCount)
	{
		synchronized(mcLock)
		{
			maxClientsCount = _maxClientsCount;
		}
	}

	public int getTimeStamp()
	{
		synchronized(tsLock)
		{
			return timeStamp;
		}
	}
	
	public void setTimeStamp(int _timeStamp)
	{
		synchronized(tsLock)
		{
			timeStamp = _timeStamp;
		}
	}
	
	public void setNodeId(int _nodeId)
	{
		synchronized(ndLock)
		{
			nodeId = _nodeId;
		}
	}
	
	/*public int getTimeStamp()
	{
		return timeStamp;
	}*/
	
	public ServerSocket getServerSocket()
	{
		return serverSocket;
	}
	
	public Socket getClientSocket()
	{
		return clientSocket;		
	}
	
	
	public String[] getNodeById(int id)
	{  
		synchronized(ndsLock)
		{
			return nodes.get(id);
		}
	}
	
	public Map<Integer,String[]> getNodes()
	{
		synchronized(ndsLock)
		{
			return nodes;
		}
	}
	

	public void readConfig()
	{
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("config.txt"));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		//	this.initialize(attributes, numberOfInstances);		
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int i = 0;
		while(line != null){
			System.out.println(line);
				line = line.trim();
				line = line.split("#")[0];
				//int[] newInstance = new int[attributes];			
				String[] words = line.split("\\s+");
				if(words.length>2)
				{
					String[] value = {words[1],words[2]};
					nodes.put(Integer.parseInt(words[0]),value);				
				}
				else
				{
					setMaxClientsCount(Integer.parseInt(words[0]));
					System.out.println("max client count: "+getMaxClientsCount());
				}
				try {
					line = br.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
	}
	
	public int max(int x, int y)
	{
		if(x>y)
			return x;
		else
			return y;
	}
	
	
	public void increaseTimeStamp()
	{
		synchronized(this.getTsCodeLock())
		{
			clientTimeStamp = this.getTimeStamp();
			clientTimeStamp++;
			this.setTimeStamp(clientTimeStamp);
		}
			
	}
	
	public void Read()
	{
		//if atleast one replica alive randomly choose a replica  of the object and read the data

		//Approach 1:
		//if( connectedNodes.count()>0)
		//   for each node n in (connectedNodes[this.nodeId] -- from faultConfig)
		//   { read data from server n using thread n }
				
		//Approach 2:
		// if( connectedNodes.count()>0)
		//   for each node n in (connectedNodes[this.nodeId] -- from faultConfig)
		//   { connectNode(n)
		//	  read data from server n }

	}
	
	public void Write()
	{
		//if atleast 2 replicas present update object value on both of them
		//	compare timestamps with other server to update value in FIFO order i.e. ensuring total ordering-->to be done by server
		//else abort with message
		
		//Approach 1:
		//if( connectedNodes.count()>0)
		//   for each node n in (connectedNodes[this.nodeId] -- from faultConfig)
		//   { write data to server n using thread n }
		
		
		//Approach 2:
		// if( connectedNodes.count()>1)
		//   for each node n in (connectedNodes[this.nodeId] -- from faultConfig)
		//   { connectNode(n)
		//	  write data to server n}
	}
	
	public void readFaultsConfig()
	{
		//scenarios for creating faults
	}
	
	public void runFaultScenario()
	{
		//Approach 1: Make all the connections and then break them
		breakLinks();
		//and
		partitionNetwork();
		
		//OR
		
		//Approach 2: Make connections with every node based on fault scenario and communicate accordingly
	}
	
	public void breakLinks()
	{
		
	}
	
	public void partitionNetwork()
	{
		
	}
	
	public void connectNode(int index)
	{
		Socket clientSocket = null;
		try {
			clientSocket = new Socket(this.getNodes().get(index)[0],Integer.parseInt(this.getNodes().get(index)[1]));
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Connecting to Server on host: "+this.getNodes().get(index)[0]+" port: "+Integer.parseInt(this.getNodes().get(index)[1]));
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(clientSocket.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		increaseTimeStamp();
		synchronized(getCnCodeLock())
		{	
			if(!getConnectedNodes().containsKey(index))
			{
				getConnectedNodes().put(index,this.getTimeStamp());
				System.out.println("Client timestamp: "+this.getTimeStamp()+" Placing Node "+index+" in request queue of "+this.getNodeId());
			}
		}
		increaseTimeStamp();
		System.out.println("Client timestamp: "+this.getTimeStamp()+" Node "+this.getNodeId()+" sending request to node "+index);
	}
	
	// Approach 1: Will require 6 server and 4 client threads for each server and 3 client threads for each client 
	public void makeConnections()
	{
		if(this.getNodeId()<7)	//if server then make connections to all other nodes
		{
			for(int i=0;i<11;i++)
				if(i!=this.getNodeId())
					connectNode(i);
		}
		else 	// else if client then make connections to all servers
		{
			for(int i=0;i<7;i++)
				connectNode(i);
		}			
	}
	
	public static void main(String args[])
	{
		NodeDS node = new NodeDS();
		InetAddress inet = null;
		try {
			inet = InetAddress.getLocalHost();
			System.out.println("Host Address: "+inet.getCanonicalHostName());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (Map.Entry<Integer, String[]> entry : node.getNodes().entrySet()) {
			System.out.println("Map value: "+entry.getValue()[0]);
		   if((entry.getValue()[0]).equals(String.valueOf(inet.getCanonicalHostName())))
		   {
			   node.setNodeId(entry.getKey());
			  System.out.println("nodeId: "+node.getNodeId());
		   }
		}
		System.out.println("Starting server at node: "+node.getNodeId());
		//Approach 2: Single client and single server thread
		Runnable r = new ServerThread(node);
		new Thread(r).start();
		try {
			Thread.sleep(45000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Starting client at node: "+node.getNodeId());
		
	}
}



