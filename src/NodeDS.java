import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;


public class NodeDS {
	private int nodeId;
	private int clientTimeStamp;
	private int timeStamp;
	private int maxObjectsCount;
	private int maxClientsCount;
	private int objectValue;
	private boolean isObjectRead;
	private ServerSocket serverSocket = null;
	private Socket clientSocket = null;
	private Map<Integer,String[]> nodes = null;
	private Map<Integer,Integer> objectStore = null;
	private Map<Integer,int[]> connectedNodes = null;
	private static final Object ndLock = new Object();
	private static final Object objValLock = new Object();
	private static final Object cnNdLock = new Object();
	private static final Object cnCodeLock = new Object();
	private static final Object tsCodeLock = new Object();
	private static final Object ndsLock = new Object();
	private static final Object mcLock = new Object();
	private static final Object tsLock = new Object();
	private static final Object objCntLock = new Object();
	private static final Object ndCnctLock = new Object();
	private static final Object obRdLock = new Object();
	private boolean[] isNodeConnected;
	public NodeDS()
	{
		timeStamp = 0;
		maxClientsCount=0;
		objectValue = 0;
		nodes = new HashMap<Integer,String[]>();
		connectedNodes = new HashMap<Integer,int[]>();
		isNodeConnected = new boolean[12];
		readConfig();
	}
	
	public Object getTsCodeLock()
	{
		return tsCodeLock;
	}
	
	public int getMaxClientsCount()
	{
		synchronized(mcLock)
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
	
	public int getObjectValue()
	{
		synchronized(objValLock)
		{
			return objectValue;
		}
	}
	
	public void setObjectValue(int _objectValue)
	{
		synchronized(objValLock)
		{
			objectValue = _objectValue;
		}
	}
	
	public boolean getIsObjectRead()
	{
		synchronized(obRdLock)
		{
			return isObjectRead;
		}
	}
	
	public void setIsObjectRead(boolean _isObjectRead)
	{
		synchronized(obRdLock)
		{
			isObjectRead = _isObjectRead;
		}
	}
	
	public int getNodeId()
	{
		synchronized(ndLock)
		{
			return nodeId;
		}
	}
	
	public int[] getConnectedNodes()
	{
		synchronized(cnNdLock)
		{
			return connectedNodes.get(this.nodeId);
		}
	}
	
	public boolean getIsNodeConnected(int index)
	{
		synchronized(ndCnctLock)
		{
			return isNodeConnected[index];
		}
	}
	
	public Object getCnCodeLock()
	{
		return cnCodeLock;
	}
	
	
	public int getMaxObjectsCount()
	{
		synchronized(objCntLock)
		{
			return maxObjectsCount;
		}
	}
	
	public void setMaxObjectsCount(int _maxObjectsCount)
	{
		synchronized(objCntLock)
		{
			maxObjectsCount = _maxObjectsCount;
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
	
	public void increaseTimeStamp()
	{
		synchronized(this.getTsCodeLock())
		{
			clientTimeStamp = this.getTimeStamp();
			clientTimeStamp++;
			this.setTimeStamp(clientTimeStamp);
		}
			
	}
	
	// Reads all node ids with corresponding ip address and socket number 
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
					System.out.println("max client count: "+getMaxObjectsCount());
				}
				try {
					line = br.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
	}
	
	
	
	
	/* Object Functions */

	// Based on object hashcode gets server to map to for reading and writing
	public ArrayList<Integer> HashObjectToServer(int id)
	{
		ArrayList<Integer> indexes =new ArrayList<Integer>();
		//int id = System.identityHashCode(O);
		int index1 = id%7;
		int index2 = (id+1)%7;
		int index3 = (id+2)%7;
		//int indexes[] = {index1,index2,index3};
		indexes.add(index1);
		indexes.add(index2);
		indexes.add(index3);
		return indexes;
	}
	
	// For creating new object to get the hashcode 
	public int createNewObject()
	{
		Object obj = new Object();
		//System.out.println("Original Hashcode: "+ System.identityHashCode(obj));
		int objectCode = System.identityHashCode(obj);
		System.out.println("Object Code: "+ System.identityHashCode(obj));
		try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("objects.txt", true)))) {
		    out.println(objectCode);
		}catch (IOException e) {
		    //exception handling left as an exercise for the reader
		}
		return objectCode;
	}
	
	// Initially store the objects on the server
	public void writeObjectsToFile()
	{
		if(this.nodeId==7)
		{	
			int maxObjects = Util.randomInRange(10,18);
			for(int i=0;i<maxObjects;i++)
			{
				int objectCode = createNewObject();
				Write(objectCode);
			}
		}
	}
	
	// For clients to read all objects 
	public ArrayList<Integer> readAllObjects()
	{
		ArrayList<Integer> objects = new ArrayList<Integer>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("objects.txt"));
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
				//line = line.split("#")[0];
				//int[] newInstance = new int[attributes];			
				String[] words = line.split("\\s+");
				objects.add(Integer.parseInt(words[0]));
			 try {
					line = br.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		setMaxObjectsCount(objects.size());
		System.out.println("max object count: "+getMaxObjectsCount());
		return objects;
	}
	
	/* Object Store functions*/
	
	// On the server, gets the object and version number of the object on that particular server
	public Map<Integer,Integer> getObjectStore()
	{
		BufferedReader br = null;
		Map<Integer,Integer> objectStore = new HashMap<Integer,Integer>();
		try {
			br = new BufferedReader(new FileReader(this.nodeId+".txt"));
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
				//line = line.split("#")[0];
				//int[] newInstance = new int[attributes];			
				String[] words = line.split("\\s+");
				if(words.length>1)
				{
					//String[] value = {words[0],words[2]};
					objectStore.put(Integer.parseInt(words[0]),Integer.parseInt(words[1]));				
				}
				try {
					line = br.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		setMaxObjectsCount(objectStore.size());
		System.out.println("max objects count: "+getMaxObjectsCount());
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return objectStore;
	}
	
	// On the server, writes the object and the initial version number of the object on that particular server
	public void writeToObjectStore(int objectCode)
	{
		try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(this.nodeId+".txt", true)))) {
		    out.println(objectCode+" 0");
		}catch (IOException e) {
		    //exception handling left as an exercise for the reader
		}
	}
	
	// On the server, updates the object version number on that particular server
	public void updateObjectStore(int objectCode)
	{
		 try {
		       // Open the file that is the first
	        // command line parameter
	        FileInputStream fstream = new FileInputStream(this.nodeId+".txt");
	        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
	        String strLine;
	        StringBuilder fileContent = new StringBuilder();
	        //Read File Line By Line
	        while ((strLine = br.readLine()) != null) {
	            // Print the content on the console
	            System.out.println(strLine);
	            String tokens[] = strLine.split(" ");
	            if (tokens.length > 0) {
	                // Here tokens[0] will have value of ID
	                if (tokens[0].equals(objectCode)) {
	                    tokens[1] = String.valueOf(Integer.parseInt(tokens[1])+1);
	                    //tokens[2] = "499";
	                    String newLine = tokens[0] + " " + tokens[1];
	                    fileContent.append(newLine);
	                    fileContent.append("\n");
	                } else {
	                    // update content as it is
	                    fileContent.append(strLine);
	                    fileContent.append("\n");
	                }
	            }
	        }
	        // Now fileContent will have updated content , which you can override into file
	        FileWriter fstreamWrite = new FileWriter(this.nodeId+".txt");
	        BufferedWriter out = new BufferedWriter(fstreamWrite);
	        out.write(fileContent.toString());
	        out.close();
	        //Close the input stream
	        //in.close();
	    } catch (Exception e) {//Catch exception if any
	        System.err.println("Error: " + e.getMessage());
	    }
	}
	
	
	//Socket clientSocket = null;

	
	// Approach 1: Will require 6 server and 4 client threads for each server and 3 client threads for each client 
	/*public void makeConnections()
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
	}*/
	
	
	

	
	
	
	/* Connect, Read Write
	 * 
	 */
	
	// making connections through boolean array to be used for physical connections as needed in code
	public void connectAllNodes()
	{
		//if this node is server, connect it to all nodes
		if(this.nodeId<7)
		{
			for(int i=0;i<isNodeConnected.length;i++)
			{
				isNodeConnected[i] = true;
			}
		}
		//if this node is client, connect it to all servers
		else
		{
			for(int i=0;i<isNodeConnected.length;i++)
			{
				if(i<7)
					isNodeConnected[i] = true;
				else
					isNodeConnected[i] = false;
			}
		}
	}
	
	// Making physical socket connection to node index
	public Socket connectNode(int index)
	{
		
		try {
			//final int timeOut = (int)TimeUnit.SECONDS.toMillis(10); // 5 sec wait period
			clientSocket = new Socket(this.getNodes().get(index)[0],Integer.parseInt(this.getNodes().get(index)[1]));
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
			return null;
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
			return null;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
			return null;
		}
		System.out.println("Connecting to Server on host: "+this.getNodes().get(index)[0]+" port: "+Integer.parseInt(this.getNodes().get(index)[1]));
		return clientSocket;
	}
	
	// Read method to read object version from server
	public void Read(int objectCode)
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
		
		boolean objectRead = false;
		ArrayList<Integer> indexes = HashObjectToServer(objectCode);
		while(!objectRead)
		{
			for(int i=0;i<indexes.size();i++)
			{
				if(isNodeConnected[indexes.get(i)])
				{
					synchronized(this.getTsCodeLock())
					{
						increaseTimeStamp();
					}
					Socket clientSocket = connectNode(indexes.get(i));
					if(clientSocket == null)
						continue;
					synchronized(this.getTsCodeLock())
					{
						increaseTimeStamp();
					}
					//read object
					PrintWriter writer = null;
					try {
						writer = new PrintWriter(clientSocket.getOutputStream());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//
					/*synchronized(getCnCodeLock())
					{	
						if(!getConnectedNodes().containsKey(index))
						{
							getConnectedNodes().put(index,this.getTimeStamp());
							System.out.println("Client timestamp: "+this.getTimeStamp()+" Placing Node "+index+" in request queue of "+this.getNodeId());
						}
					}*/
					//increaseTimeStamp();
					System.out.println("Client timestamp: "+this.getTimeStamp()+" Node "+this.getNodeId()+" sending READ request to node "+indexes.get(i));
					synchronized(this.getTsCodeLock())
					{
						increaseTimeStamp();
					}
					writer.println(getTimeStamp()+":"+this.getNodeId()+":read:"+objectCode);
					writer.close();		
					while(true)
					{
						// Waiting for object value to be returned to receiver thread from server
						if(getIsObjectRead())
						{
							System.out.println("Object: "+objectCode+" Value: "+getObjectValue());
							objectRead = true;
							break;
						}
					}
					break;
				}
			}
		}
	/*	if( connectedNodes.size()>0)
		{
			for(int i=0;i<connectedNodes.size();i++) 
		    { 
				connectNode(i);
		    }
		}	*/	
	}
	
	// Method on server to send read reply of object version from server to requesting client
	public void sendReadReply(Integer objectValue,Integer nodeId)
	{
		Socket clientSocket = null;
		boolean sentReply = false;
		while(sentReply == false)
		{	
			clientSocket = connectNode(nodeId);
			if(clientSocket==null)
				continue;
	    	//write object to j
	    	  PrintWriter writer = null;
				try {
					writer = new PrintWriter(clientSocket.getOutputStream());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//
				
				synchronized(this.getTsCodeLock())
				{
					increaseTimeStamp();
				}
				/*synchronized(getCnCodeLock())
				{	
					if(!getConnectedNodes().containsKey(index))
					{
						getConnectedNodes().put(index,this.getTimeStamp());
						System.out.println("Client timestamp: "+this.getTimeStamp()+" Placing Node "+index+" in request queue of "+this.getNodeId());
					}
				}*/
				//increaseTimeStamp();
				System.out.println("Server timestamp: "+this.getTimeStamp()+" Node "+this.getNodeId()+" sending reply to node "+nodeId);
				synchronized(this.getTsCodeLock())
				{
					increaseTimeStamp();
				}
				writer.println(getTimeStamp()+":"+this.getNodeId()+":readReply:"+objectValue);
				sentReply = true;
				writer.close();		
		}
	}
	
	// Method on client to WRITE or UPDATE object version on server based on hashcode response
	public void Write(int objectCode)
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
		ArrayList<Integer> connectedNodes = new ArrayList<Integer>();
		ArrayList<Integer> indexes = HashObjectToServer(objectCode);
		for(int i=0;i<indexes.size();i++)
		{
			if(isNodeConnected[indexes.get(i)])
			{
				connectedNodes.add(indexes.get(i));
			}
		}
		int objectWrittenCount = 0;
		boolean objectWritten[] = {false,false,false};
		while((objectWrittenCount<3 && connectedNodes.size()==3)||((objectWrittenCount<2 && connectedNodes.size()==2)))
		{
		      for(int j=0;j<connectedNodes.size();j++)	
		      {
		    	  if(!objectWritten[connectedNodes.get(j)])
		    	  {
		    	  Socket clientSocket = connectNode(connectedNodes.get(j));
		    	  if(clientSocket==null)
		    		  continue;
		    	//write object to j
		    	  PrintWriter writer = null;
					try {
						writer = new PrintWriter(clientSocket.getOutputStream());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//
					synchronized(this.getTsCodeLock())
					{
						increaseTimeStamp();
					}
					/*synchronized(getCnCodeLock())
					{	
						if(!getConnectedNodes().containsKey(index))
						{
							getConnectedNodes().put(index,this.getTimeStamp());
							System.out.println("Client timestamp: "+this.getTimeStamp()+" Placing Node "+index+" in request queue of "+this.getNodeId());
						}
					}*/
					//increaseTimeStamp();
					System.out.println("Client timestamp: "+this.getTimeStamp()+" Node "+this.getNodeId()+" sending WRITE request to node "+connectedNodes.get(j));
					synchronized(this.getTsCodeLock())
					{
						increaseTimeStamp();
					}
					writer.println(getTimeStamp()+":"+this.getNodeId()+":write:"+objectCode);
					objectWritten[connectedNodes.get(j)] = true;
					writer.close();		
		    	  }
		      }
		}
	}
	
	
	/* Fault Creation and Handling */
	
	// read/write scenarios
	public void runTestScenario(ArrayList<Integer> objectCodes, int totalTests)
	{
		int index; 
		for(int i=0;i<totalTests;i++)
		{
			index = Util.randomInRange(0,objectCodes.size()-1);
			Read(objectCodes.get(index));
			//index = randomInRange(0,objectCodes.size()-1);
			//Write(objectCodes.get(index));
		}
	}
	
	 public void readFaultsConfig()
	{
		//scenarios for creating faults i.e. reading partition1 and partition2 arrays for partitionNetwork method
	}
	
	
	
	public void runFaultScenario()
	{
		//Approach 1: Make all the connections and then break them
		//and
		//OR
		//Approach 2: Make connections with every node based on fault scenario and communicate accordingly
		
		// 0-6: Server 7-11: Client
		int[] partition1 = {0,2,4,6,10};
		int[] partition2 = {1,3,5,7,9,11};
		partitionNetwork(partition1,partition2);
		
		//breakLink(2,10);
	}
	
	public void breakLink(int node1,int node2)
	{
		int numPart1 = Util.randomInRange(2,6);
		int numPart2 = 12 - numPart1;
		ArrayList<Integer> nodeIds = new ArrayList<Integer>();
		ArrayList<Integer> partition1 = new ArrayList<Integer>();
		partition1.add(node1);
		ArrayList<Integer> partition2 = new ArrayList<Integer>();
		partition2.add(node2);
		int totalServers = 7;
		int totalClients = 5;
		int totalAvailableServers =6;
		int part1Servers = 0,part2Servers = 0,part1Clients=0,part2Clients=0;
		int totalAvailableClients = 4;
		if(node1<7)
		{
			part2Servers = Util.randomInRange(1,numPart2-1);
			part2Clients = numPart2 - part2Servers;
			part1Clients = Util.randomInRange(1,numPart1-1);
			part1Servers = numPart1 - part1Clients;
		}
		
		for(int i=0;i<part1Servers;i++)
		{
			int newNode; 
			do
			{
			 newNode = Util.randomInRange(0,6);
			}while(partition2.contains(newNode)||partition1.contains(newNode));
			partition1.add(newNode);
		}
		for(int i=0;i<part2Servers;i++)
		{
			int newNode; 
			do
			{
			 newNode = Util.randomInRange(0,6);
			}while(partition2.contains(newNode)||partition1.contains(newNode));
			partition2.add(newNode);
		}
		for(int i=0;i<part1Clients;i++)
		{
			int newNode; 
			do
			{
			 newNode = Util.randomInRange(7,11);
			}while(partition2.contains(newNode)||partition1.contains(newNode));
			partition1.add(newNode);
		}
		for(int i=0;i<part2Clients;i++)
		{
			int newNode; 
			do
			{
			 newNode = Util.randomInRange(7,11);
			}while(partition2.contains(newNode)||partition1.contains(newNode));
			partition2.add(newNode);
		}
		/*if(numPart1==2)
		{
			if(node1<7)
			{
				int newNode; 
				do
				{
					newNode = randomInRange(7,11);
				}while(partition2.contains(newNode));
				partition1.add(newNode);
				totalAvailableClients--;
			}
			else
			{
				int newNode; 
				do
				{
				 newNode = randomInRange(0,6);
				}while(partition2.contains(newNode));
				partition1.add(newNode);
				totalAvailableServers--;
		    }
		}
		 else
		 {
			 if(node1 <7)
			 {
				 
			 }
			 else
			 {
				 
			 }
		 }
		
		for(int i=0;i<numPart2-1;i++)
		{
			if(numPart2==2)
			{
				if(node2<7)
				{
					int newNode; 
					do
					{
						newNode = randomInRange(7,11);
					}while(partition1.contains(newNode));
					partition2.add(newNode);
					totalAvailableClients--;
				}
				else
				{
					int newNode; 
					do
					{
					 newNode = randomInRange(0,6);
					}while(partition1.contains(newNode));
					partition2.add(newNode);
					totalAvailableServers--;
			    }
			 }
			else
			{
				
			}
		}*/
		int[] part1 = Util.convertIntegers(partition1);
		int[] part2 = Util.convertIntegers(partition2);
		partitionNetwork(part1,part2);		
	}
	
	
	
	public void partitionNetwork(int[] partition1,int[] partition2)
	{
		int disconnectedPartition = 0; 
		for(int i=0;i<partition1.length;i++)
		{
			if(this.nodeId==partition1[i])
			{
				disconnectedPartition = 2;
				break;
			}				
		}
		if(disconnectedPartition==0)
		{
			disconnectedPartition =1;
		}
		if(disconnectedPartition==1)
		{
			for(int j=0;j<partition1.length;j++)
			{
				isNodeConnected[partition1[j]]=false;
			}
		}
		else
		{
			for(int j=0;j<partition2.length;j++)
			{
				isNodeConnected[partition2[j]]=false;
			}
		}
	} 
	  
	  
	 
	
	public static void main(String args[])
	{
		NodeDS node = new NodeDS();
		node.connectAllNodes();
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
		if(node.getNodeId()>6)
		{
			//node.writeObjectsToFile();
			try {
				Thread.sleep(45000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ArrayList<Integer> objectCodes = node.readAllObjects();
			node.runTestScenario(objectCodes,5);
		}
	}
}



