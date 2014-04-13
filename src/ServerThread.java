import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


public class ServerThread implements Runnable {

  private NodeDS node = null;
  private int serverTimeStamp = -1;
  int totalNodesMessages[];
  private ServerSocket serverSock; 
  private Socket clientSocket = null;
   public ServerThread(Object parameter) {
       // store parameter for later user
	   node = (NodeDS)parameter;
	   totalNodesMessages = new int[node.getMaxClientsCount()];
		try {
			serverSock = new ServerSocket(Integer.parseInt(node.getNodes().get(node.getNodeId())[1]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	   serverTimeStamp = node.getTimeStamp();
	   serverTimeStamp++;
	   node.setTimeStamp(serverTimeStamp);
   }
   
   public void startServer()
	{
		String message = null;
		try
		{
			//Create a server socket at port 5000
			System.out.println("Server started on: "+Integer.parseInt(node.getNodes().get(node.getNodeId())[1]));
			synchronized(node.getTsCodeLock())
			{
				increaseTimeStamp();
			}
			System.out.println("Server timestamp: "+node.getTimeStamp()+" at node: "+node.getNodeId());
			//Server goes into a permanent loop accepting connections from clients			
			while(true)
			{
				//Listens for a connection to be made to this socket and accepts it
				//The method blocks until a connection is made
				Socket sock = serverSock.accept();
				
				synchronized(node.getTsCodeLock())
				{
					increaseTimeStamp();
				}
				System.out.println("Server timestamp: "+node.getTimeStamp()+" Node "+node.getNodeId()+" accepted connection");
				BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				message = reader.readLine();
				
				synchronized(node.getTsCodeLock())
				{
					increaseTimeStamp();
				}
				System.out.println("Server timestamp: "+node.getTimeStamp()+" Node "+node.getNodeId()+" read message from client");
				String messageParts[] = message.split(":");
				
				synchronized(node.getTsCodeLock())
				{
					serverTimeStamp = max(node.getTimeStamp(),Integer.parseInt(messageParts[0]));
					node.setTimeStamp(serverTimeStamp);
				}				
				System.out.println("Server timestamp: "+node.getTimeStamp()+" Client says: " + message);
				System.out.println("Server timestamp: "+node.getTimeStamp()+" max of message from client "+messageParts[1]+" and server "+node.getNodeId()+" timestamp");
				
			
				sock.close();
			}
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
	}

   public void run() {
	   startServer();
   }
}
