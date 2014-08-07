package polka.netdispatch;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import polka.ifc.NetworkCallInbound;
import polka.util.WorkUnit;

/**
 * Analyzes a connection and transitions to other WorkUnit to 
 * perform some work on it later
 * 
 * @author Henrietta
 *
 */
public class DispatchSingleConnection extends WorkUnit {

	private SocketChannel sc;
	
	public DispatchSingleConnection(SocketChannel sc) {
		this.sc = sc;
	}
	
	public void runLogic() throws IOException {		
		new NetworkCallInbound(this.sc).run();
	}
	
	@Override
	public void run() {
		try {
			this.runLogic();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
