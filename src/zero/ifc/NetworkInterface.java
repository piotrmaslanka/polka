package zero.ifc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import zero.store.SeriesDefinition;

public class NetworkInterface implements SystemInterface {

	private Socket sock;
	private DataInputStream dis;
	private DataOutputStream dos;
	
	/**
	 * Connect to target node.
	 */
	public NetworkInterface(InetSocketAddress addr) throws IOException {
		this.sock = new Socket(addr.getAddress().getHostAddress(), addr.getPort());
		this.sock.setSoTimeout(10);
		
		this.dis = new DataInputStream(this.sock.getInputStream());
		this.dos = new DataOutputStream(this.sock.getOutputStream());
		
		this.dos.writeByte((byte)2);
		this.dos.flush();
	}
	
	@Override
	public void close() throws IOException {
		this.sock.close();
	}
	
	@Override
	public void writeSeries(SeriesDefinition sd, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		try {
			this.dos.writeByte((byte)3);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.writeLong(prev_timestamp);
			this.dos.writeLong(cur_timestamp);
			this.dos.writeInt(data.length);
			this.dos.write(data);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 0) return;
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			if (result == 4) throw new IllegalArgumentException();
			throw new IOException();
		} catch (IOException e) {
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			throw new IOException();
		}
	}
	
	@Override
	public long getHeadTimestamp(SeriesDefinition seriesname) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException {
		try {
			this.dos.writeByte((byte)2);
			seriesname.toDataStreamasINTPRepresentation(this.dos);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			
			if (result == 0)
				return this.dis.readLong();
			else
				throw new IOException();			
		} catch (IOException e) {
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			throw new IOException();
		}
	}
	
	@Override
	public void updateDefinition(SeriesDefinition sd) throws LinkBrokenException, IOException {
		try {
			this.dos.writeByte((byte)1);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 0)
				return;
			else
				throw new IOException();			
		} catch (IOException e) {
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			throw new IOException();
		}
	}
	
	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		try {
			this.dos.writeByte((byte)0);
			this.dos.writeUTF(seriesname);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 0)
				return SeriesDefinition.fromDataStreamasINTPRepresentation(this.dis);
			else
				throw new IOException();
		} catch (IOException e) {
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			throw new IOException();
		}
	}
	
	@Override
	public void read(SeriesDefinition sd, long from, long to, WritableByteChannel channel)
		throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException {
		
		try {
			this.dos.writeByte((byte)4);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.writeLong(from);
			this.dos.writeLong(to);
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			if (result == 4) throw new IllegalArgumentException();
			
			// rolling in
			long ts = this.dis.readLong();
			ByteBuffer record = ByteBuffer.allocate(sd.recordSize+8);
			byte[] rec = new byte[sd.recordSize];
			while (ts != -1) {
				// roll one record
				record.putLong(ts);
				this.dis.readFully(rec);
				record.put(rec);
				record.flip();	
				channel.write(record);
				
				// read next timestamp
				ts = this.dis.readLong();
			}

		} catch (IOException e) {
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			throw new IOException();
		}
	}
}
