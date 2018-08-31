package com.hyw.SDS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class Slave2 {
	// ��Ҫ������Ŀ�����ݿ�
	File destfile = new File("slavedb2.xml");
	Map<String, String> slavedb = new HashMap<>();
	
	
	
	boolean flag = true;
	private static SocketChannel socket;
	
	
	// �����̣߳��Զ�����ͬ��
	public void run() {
		new Thread(new MyThread()).start();
	}


	// ����ͬ���߳�
	private class MyThread implements Runnable {

		@Override
		public void run() {
			// Auto-generated method stub
			while (flag) {
				// ����ͬ����������
				//flag = false;
				try {
					initS2M("localhost", 8002);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				send("sync");
				receivedb();
				
				try {
					Thread.sleep(1000 * 3);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	
	// ��ʼ���ӷ�����
	private void initS2M(String ip, int port) throws IOException {
		// ���һ��Socketͨ��
		socket = SocketChannel.open();
		// Socket����
		socket.connect(new InetSocketAddress(ip, port));
		// ͨ������Ϊ������
		socket.configureBlocking(false);
		
	}
	
	// �������ݿ�ͬ����Ϣ��master
	private void send(String msg) {
		try {
			socket.write(ByteBuffer.wrap(msg.getBytes()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// �������ݿ�ͬ����Ϣ������
	private void receivedb(){

		String msg = null;
		boolean getted = true;
		while (getted) {
			try {
				// ������ȡ�Ļ�����
				ByteBuffer buffer = ByteBuffer.allocate(1024);
				// �Ӹ�ͨ�����ֽ����ж�������Ļ�������
				socket.read(buffer);
				// ���������ֽ�����ķ�ʽ���Ƹ�data
				byte[] data = buffer.array();
				// ת�����ַ�������ȥ�ո�
				msg = new String(data).trim();

				if (!msg.isEmpty()) {
					//flag = true;
//					System.out.println("�ӷ�����յ���Ϣ��" + msg);
					
					FileOutputStream outputStream = new FileOutputStream(destfile);
					// �и�ʽ���пո� �׶�
					OutputFormat format = OutputFormat.createPrettyPrint();
					// �޿ո���,��Լ�ռ�
					// OutputFormat format=OutputFormat.createCompactFormat();

					format.setEncoding("UTF-8");
					XMLWriter writer = new XMLWriter(outputStream, format);

					writer.write(DocumentHelper.parseText(msg));
					writer.close();
					initmap(destfile);
					getted = false;
				}
			} catch (Exception e) {
				// Auto-generated catch block
				System.out.println("xml�ļ������쳣");
				e.printStackTrace();
				try {
					socket.socket().close();
					socket.close();
					break;
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			}
	}
	
	// ��ȡ�ļ����ڴ���
	public void initmap(File f) {
		SAXReader r = new SAXReader();
		try {
			org.dom4j.Document doc = r.read(f);

			Element e = doc.getRootElement();
			// System.out.println("sdjf");
			Iterator<Element> e1 = e.elementIterator();// ���ڵ��µ��ӽڵ㼯��
	//		System.out.println(e1.hasNext());
			while (e1.hasNext()) {
				// System.out.println("sdf");
				Element tmpe = e1.next();
				if (tmpe.attributeValue("name").equals("kv")) {
					slavedb.put(tmpe.element("key").getText(), tmpe.element("value").getText());
			//		System.out.println(slavedb.get("qqqqq"));
					
					//			System.out.println(tmpe.element("key").getText());
		//			System.out.println(tmpe.element("value").getText());
					// System.out.println("sdfwe");
				}
			}
		} catch (DocumentException e) {
			//  �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}

	// ˽�л�һ��Slave�ϵ�selector
	private Selector selector;

	// ����selector�˿ڣ�����C2S���ٶ�ȡ����
	public void initS2C(int port) throws IOException {
		// �½�һ��socket
		ServerSocketChannel socket = ServerSocketChannel.open();
		// ��serversocketchannel��һ��port
		socket.socket().bind(new InetSocketAddress(port));

		// ע��һ��ͨ��������ͨ�����ڿɽ���״̬
		selector = selector.open();
		socket.configureBlocking(false);
		SelectionKey key = socket.register(selector, SelectionKey.OP_ACCEPT);

		System.out.println("Slaver����ͨ��������ɣ�");
	}

	// Slave��ʼ����channel
	public void listen() throws IOException {
		while (true) {
			System.out.println("selector������");
			// ����select()���������û��ע���¼����������
			selector.select();
			System.out.println("��C����S��");
			// ��������ȡselector�е�key
			Iterator<SelectionKey> itor = this.selector.selectedKeys().iterator();
			while (itor.hasNext()) {
				SelectionKey key = itor.next();
				itor.remove();
				// �ж�key�ľ���״̬�����в�ͬ����

				// ������״̬----Slave���ṩһ��channel��Client������
				if (key.isAcceptable()) {
					ServerSocketChannel socket = (ServerSocketChannel) key.channel();
					SocketChannel serverchannel = socket.accept();
					serverchannel.configureBlocking(false);
					serverchannel.register(this.selector, SelectionKey.OP_READ);
					System.out.println("�ı�Channel״̬Ϊ�ɶ�");
				}
				// �ɶ�ȡ״̬----Slave�Ѿ���ע��channel���ӣ����ж�ȡchannel
				else if (key.isReadable()) {
					System.out.println("0");
					// �ҵ�key��Ӧ��channel
					SocketChannel serverchannel = (SocketChannel) key.channel();
					// ����buffer����ȡchannel���е�����
					System.out.println("1");
					ByteBuffer inmsg = ByteBuffer.allocate(1024);
					inmsg.clear();
					// �Ӹ�ͨ�����ֽ����ж�������Ļ�����
					System.out.println("2");
					serverchannel.read(inmsg);
					System.out.println("3");
					byte[] data = inmsg.array();
					System.out.println("4");
					String msg = new String(data).trim();
					System.out.println("Slave�յ���Ϣ��" + msg);

					// �����յ������ݲ��ҽ��в���
					String getC2Mstring = msg;
					String[] arg = strsplit(getC2Mstring); // �õ��ָ�������

					ByteBuffer outmsg = ByteBuffer.wrap(methods(arg, slavedb).getBytes());
					serverchannel.write(outmsg);
					serverchannel.close();
				}

			}
		}
	}

	// ���õ������������з�Ƭ����---���ݿո���з�Ƭ
	public static String[] strsplit(String str) {
		String[] arg = str.split(" ");
		return arg;
	}

	// �����ȽϺ���
	public String methods(String[] arg, Map<String, String> map) {
		String str = "0";
		if (arg[0].equals("get")) {
			str = get(arg, map);
			System.out.println(str);
			return str;
		} else if (arg[0].equals("mget")) {
			str = mget(arg, map);
			System.out.println(str);
			return str;
//		} else if (arg[0].equals("getrange")) {
//			str = getrange(arg, map);
//			System.out.println(str);
//			return str;
		}
		return str;
	}

	// get������ʵ��----����key����Ӧ��value
	public static String get(String[] arg, Map<String, String> map) {
		String value = "error!";
		if (arg.length != 2) {
			System.out.println("get�����������");
		} else {
			if (map.containsKey(arg[1])) {
				value = map.get(arg[1]);
			} else {
				value = "�����ڸ�key-value!";
			}
		}
		return value;
	}

	// mget������ʵ��----һ�η��ض��key
	public static String mget(String[] arg, Map<String, String> map) {
		String[] mstr = new String[arg.length - 1];
		for (int i = 1; i < arg.length; i++) {
			mstr[i - 1] = map.get(arg[i]);
		}
		return String.join("\n", mstr);
	}

	// �ӷ�����������
	public static void main(String[] args) {
		Slave2 Slave2 = new Slave2();
		try {
			// Slave�ĳ�ʼ�������Ҹ����Զ����Ӹ��ƣ�3s���һ��
			Slave2.run();
			// ���ڼ����ͻ��˷���
			Slave2.initS2C(8012);
			Slave2.listen();
			
		} catch (IOException e) {
			//  �Զ����ɵ� catch ��
			e.printStackTrace();
		}
	}

}
