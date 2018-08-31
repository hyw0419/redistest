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
	// 需要拷贝的目的数据库
	File destfile = new File("slavedb2.xml");
	Map<String, String> slavedb = new HashMap<>();
	
	
	
	boolean flag = true;
	private static SocketChannel socket;
	
	
	// 设置线程，自动主从同步
	public void run() {
		new Thread(new MyThread()).start();
	}


	// 主从同步线程
	private class MyThread implements Runnable {

		@Override
		public void run() {
			// Auto-generated method stub
			while (flag) {
				// 发送同步请求命令
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

	
	// 初始化从服务器
	private void initS2M(String ip, int port) throws IOException {
		// 获得一个Socket通道
		socket = SocketChannel.open();
		// Socket连接
		socket.connect(new InetSocketAddress(ip, port));
		// 通道设置为非阻塞
		socket.configureBlocking(false);
		
	}
	
	// 发送数据库同步信息给master
	private void send(String msg) {
		try {
			socket.write(ByteBuffer.wrap(msg.getBytes()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 接受数据库同步信息并保存
	private void receivedb(){

		String msg = null;
		boolean getted = true;
		while (getted) {
			try {
				// 创建读取的缓冲区
				ByteBuffer buffer = ByteBuffer.allocate(1024);
				// 从该通道将字节序列读入给定的缓冲区。
				socket.read(buffer);
				// 缓存区以字节数组的方式复制给data
				byte[] data = buffer.array();
				// 转化成字符串，除去空格
				msg = new String(data).trim();

				if (!msg.isEmpty()) {
					//flag = true;
//					System.out.println("从服务端收到信息：" + msg);
					
					FileOutputStream outputStream = new FileOutputStream(destfile);
					// 有格式，有空格 易读
					OutputFormat format = OutputFormat.createPrettyPrint();
					// 无空格换行,节约空间
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
				System.out.println("xml文件拷贝异常");
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
	
	// 读取文件到内存中
	public void initmap(File f) {
		SAXReader r = new SAXReader();
		try {
			org.dom4j.Document doc = r.read(f);

			Element e = doc.getRootElement();
			// System.out.println("sdjf");
			Iterator<Element> e1 = e.elementIterator();// 根节点下的子节点集合
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
			//  自动生成的 catch 块
			e.printStackTrace();
		}
	}

	// 私有化一个Slave上的selector
	private Selector selector;

	// 设置selector端口，方便C2S快速读取数据
	public void initS2C(int port) throws IOException {
		// 新建一个socket
		ServerSocketChannel socket = ServerSocketChannel.open();
		// 给serversocketchannel绑定一个port
		socket.socket().bind(new InetSocketAddress(port));

		// 注册一个通道并且让通道出于可接受状态
		selector = selector.open();
		socket.configureBlocking(false);
		SelectionKey key = socket.register(selector, SelectionKey.OP_ACCEPT);

		System.out.println("Slaver监听通道启动完成！");
	}

	// Slave开始监听channel
	public void listen() throws IOException {
		while (true) {
			System.out.println("selector阻塞！");
			// 调用select()方法，如果没有注册事件到达，则阻塞
			selector.select();
			System.out.println("有C到达S！");
			// 迭代器读取selector中的key
			Iterator<SelectionKey> itor = this.selector.selectedKeys().iterator();
			while (itor.hasNext()) {
				SelectionKey key = itor.next();
				itor.remove();
				// 判断key的就绪状态，进行不同处理

				// 可连接状态----Slave端提供一个channel与Client端连接
				if (key.isAcceptable()) {
					ServerSocketChannel socket = (ServerSocketChannel) key.channel();
					SocketChannel serverchannel = socket.accept();
					serverchannel.configureBlocking(false);
					serverchannel.register(this.selector, SelectionKey.OP_READ);
					System.out.println("改变Channel状态为可读");
				}
				// 可读取状态----Slave已经与注册channel连接，进行读取channel
				else if (key.isReadable()) {
					System.out.println("0");
					// 找到key对应的channel
					SocketChannel serverchannel = (SocketChannel) key.channel();
					// 设置buffer来获取channel内中的数据
					System.out.println("1");
					ByteBuffer inmsg = ByteBuffer.allocate(1024);
					inmsg.clear();
					// 从该通道将字节序列读入给定的缓冲区
					System.out.println("2");
					serverchannel.read(inmsg);
					System.out.println("3");
					byte[] data = inmsg.array();
					System.out.println("4");
					String msg = new String(data).trim();
					System.out.println("Slave收到信息：" + msg);

					// 处理收到的数据并且进行操作
					String getC2Mstring = msg;
					String[] arg = strsplit(getC2Mstring); // 得到分割后的数据

					ByteBuffer outmsg = ByteBuffer.wrap(methods(arg, slavedb).getBytes());
					serverchannel.write(outmsg);
					serverchannel.close();
				}

			}
		}
	}

	// 将得到的数据流进行分片处理---根据空格进行分片
	public static String[] strsplit(String str) {
		String[] arg = str.split(" ");
		return arg;
	}

	// 方法比较函数
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

	// get方法的实现----返回key所对应的value
	public static String get(String[] arg, Map<String, String> map) {
		String value = "error!";
		if (arg.length != 2) {
			System.out.println("get参数输入错误！");
		} else {
			if (map.containsKey(arg[1])) {
				value = map.get(arg[1]);
			} else {
				value = "不存在该key-value!";
			}
		}
		return value;
	}

	// mget方法的实现----一次返回多个key
	public static String mget(String[] arg, Map<String, String> map) {
		String[] mstr = new String[arg.length - 1];
		for (int i = 1; i < arg.length; i++) {
			mstr[i - 1] = map.get(arg[i]);
		}
		return String.join("\n", mstr);
	}

	// 从服务器主方法
	public static void main(String[] args) {
		Slave2 Slave2 = new Slave2();
		try {
			// Slave的初始化，并且负责自动主从复制，3s完成一次
			Slave2.run();
			// 用于监听客户端访问
			Slave2.initS2C(8012);
			Slave2.listen();
			
		} catch (IOException e) {
			//  自动生成的 catch 块
			e.printStackTrace();
		}
	}

}
