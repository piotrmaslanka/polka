package zero.netdispatch;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;

import zero.startup.ConfigManager;

/**
 * Thread that takes in and dispatches all inbound connections
 * that appear on node's IP or UNIXSocket
 * @author Henrietta
 *
 */
public class DispatcherThread extends Thread {
	
	final private boolean isUNIXdomain;
	
	public void terminate() {
		this.interrupt();
	}
	
	public DispatcherThread(boolean isUNIXDomain) {
		this.isUNIXdomain = isUNIXDomain;
	}
	public DispatcherThread() { this.isUNIXdomain = false; }
	
	public void runLogic() throws IOException {
		ServerSocketChannel sc;
		if (!this.isUNIXdomain) {
			sc = ServerSocketChannel.open();
			sc.bind(ConfigManager.get().node_interface);
		} else {
			File socketFile = new File(ConfigManager.get().unix_socket_name);
			socketFile.delete();
			
			// TODO: implement this!
			return;
			/**
			socketFile = new File(ConfigManager.get().unix_socket_name);
			AFUNIXServerSocket sock = AFUNIXServerSocket.newInstance();
			sock.bind(new AFUNIXSocketAddress(socketFile));
			sc = sock.getChannel();**/
		}
		
		while (!Thread.interrupted())
			new DispatchSingleConnection(sc.accept()).executeAsThread();
	}
	
	@Override
	public void run() {
		try {
			this.runLogic();
		} catch (ClosedByInterruptException e) {}
		  catch (IOException e) {
			  e.printStackTrace();
		  }
	}

}
