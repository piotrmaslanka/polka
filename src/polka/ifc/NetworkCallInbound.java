package polka.ifc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import polka.store.SeriesDefinition;
import polka.util.WorkUnit;

/**
 * work unit servicing an interface call
 *
 */
public class NetworkCallInbound extends WorkUnit {

	private SocketChannel sc;
	private LocalInterface ifc;
	
	public NetworkCallInbound(SocketChannel sc) {
		this.sc = sc;
		this.ifc = new LocalInterface();
	}
	
	@Override
	public void run() throws IOException {
		Socket sc = this.sc.socket();
		this.sc.configureBlocking(true);
		sc.setSoTimeout(5000);
		DataInputStream dis = new DataInputStream(sc.getInputStream());
		DataOutputStream dos = new DataOutputStream(sc.getOutputStream());
		
		try {
			while (true) {
				byte command = dis.readByte();
				
				if (command == 0) {										// Get definition
					String seriesName = dis.readUTF();
					SeriesDefinition sd = null;
					try {
						sd = ifc.getDefinition(seriesName);
						if (sd == null)
							dos.writeByte((byte)2);
						else {
							dos.writeByte((byte)0);
							sd.toDataStreamasINTPRepresentation(dos);
						}
					} catch (IOException e) {
						dos.writeByte((byte)1);
					}	
				}
				else if (command == 1) {								// Update definition
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						ifc.updateDefinition(sd);
						dos.writeByte((byte)0);
					} catch (IOException e) {
						dos.writeByte((byte)1);
					}
				}
				else if (command == 2) {								// Get head timestamp
					String name = dis.readUTF(); 	
					try {
						long head = ifc.getHeadTimestamp(name);
						dos.writeByte((byte)0);
						dos.writeLong(head);
					} catch (IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					}		
				}
				else if (command == 3) {								// Write series
					String name = dis.readUTF(); 
					long curt = dis.readLong();
					byte[] data = new byte[dis.readInt()];
					dis.readFully(data);
					
					try {
						ifc.writeSeries(name, curt, data);
						dos.writeByte((byte)0);
					} catch (IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (IllegalArgumentException e) {
						dos.writeByte((byte)4);
					}
				} else if (command == 4) {								// Read
					String name = dis.readUTF(); 
					long from = dis.readLong();
					long to = dis.readLong();
					
					try {
						ifc.read(name, from, to, this.sc);
					} catch (IOException e) {
						dos.writeLong(-2);
					} catch (SeriesNotFoundException e) {
						dos.writeLong(-3);
					} catch (IllegalArgumentException e) {
						dos.writeLong(-4);
					}												
				} else if (command == 5) {								// Read head
					String name = dis.readUTF(); 
					try {
						ifc.readHead(name, this.sc);
					} catch (IOException e) {
						dos.writeLong(-2);
					} catch (SeriesNotFoundException e) {
						dos.writeLong(-3);
					}
				} else if (command == 6) {								// Delete series
					String name = dis.readUTF();
					try {
						ifc.deleteSeries(name);
						dos.writeByte((byte)0);
					} catch (IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					}
				}
				
				dos.flush();
			}
		} catch (Exception e) {
		} finally {
			sc.close();
			ifc.close();
		}
	}

}
