package zero.ifc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import zero.store.SeriesDefinition;
import zero.util.WorkUnit;

/**
 * work unit servicing an interface call
 *
 */
public class NetworkCallInbound extends WorkUnit {

	private SocketChannel sc;
	private SystemInterface ifc;
	
	public NetworkCallInbound(SocketChannel sc, SystemInterface ifc) {
		this.sc = sc;
		this.ifc = ifc;
	}
	
	@Override
	public void run() throws IOException {
		Socket sc = this.sc.socket();
		this.sc.configureBlocking(true);
		sc.setSoTimeout(10000);
		DataInputStream dis = new DataInputStream(sc.getInputStream());
		DataOutputStream dos = new DataOutputStream(sc.getOutputStream());
		
		try {
			while (true) {
				byte command = dis.readByte();
				
				if (command == 0) {
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
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					}	
					dos.flush();
				}
				else if (command == 1) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						ifc.updateDefinition(sd);
						dos.writeByte((byte)0);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					}
					dos.flush();
				}
				else if (command == 2) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						long head = ifc.getHeadTimestamp(sd);
						dos.writeByte((byte)0);
						dos.writeLong(head);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						dos.writeByte((byte)3);
					}
					dos.flush();				
				}
				else if (command == 3) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					long prevt = dis.readLong();
					long curt = dis.readLong();
					byte[] data = new byte[dis.readInt()];
					dis.readFully(data);
					
					try {
						ifc.writeSeries(sd, prevt, curt, data);
						dos.writeByte((byte)0);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						dos.writeByte((byte)3);
					} catch (IllegalArgumentException e) {
						dos.writeByte((byte)4);
					}
					dos.flush();
				} else if (command == 4) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					long from = dis.readLong();
					long to = dis.readLong();
					
					try {
						dos.writeByte((byte)0);
						ifc.read(sd, from, to, this.sc);
						dos.writeLong(-1);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						dos.writeByte((byte)3);
					} catch (IllegalArgumentException e) {
						dos.writeByte((byte)4);
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
