package com.hyw.SDS;

import java.awt.BorderLayout;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class Client extends JFrame {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根
		Client client = new Client("xx");

	}

	int mi = 1, mj = 1, mk = 1;
	int si = 1, sj = 1, sk = 1;
	int port0 = 8000;
	int port1 = 8001;
	int port2 = 8002;
	int sport0 = 8010;
	int sport1 = 8011;
	int sport2 = 8012;
	ArrayList<String> al = new ArrayList<>();
	String po0 = String.valueOf(port0);
	String po1 = String.valueOf(port1);
	String po2 = String.valueOf(port2);

	JFrame jf;
	JPanel jp;
	TextField tf = new TextField(30);
	TextArea ta = new TextArea();
	JLabel bq;
	JButton an;

	String str = "dsd";

	boolean beconnected = false;
	String username = null;

	public Client(String name) {
		// TODO 自动生成的构造函数存根
		username = name;
		JFrame jf = new JFrame("客户端");
		jf.setLocation(600, 400);
		jf.setSize(600, 500);
		jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		jf.setResizable(false);

		jf.setLayout(new BorderLayout());
		// 窗口关闭监听
		jf.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				// channel.socket().close();
				// channel.close();
				beconnected = false;
				System.exit(0);
			}
		});

		// TextArea ta = new TextArea();
		jf.add(ta, BorderLayout.CENTER);
		ta.setEditable(false);
		ta.setFont(new java.awt.Font("Dialog", 1, 15));
		JPanel jp = new JPanel();
		jp.setFont(new java.awt.Font("Dialog", 1, 20));
		jf.add(jp, BorderLayout.SOUTH);
		// TextField tf = new TextField(40);
		JLabel bq = new JLabel("请输入命令：");
		bq.setFont(new java.awt.Font("Dialog", 1, 18));
		JButton an = new JButton("发送");
		an.setFont(new java.awt.Font("Dialog", 1, 18));
		// 发送按钮增加事件监听
		an.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {

				sendMsg(tf.getText().trim());
				str = username + ':' + tf.getText().trim() + '\n';
				ta.append(str);
				tf.setText("");

			}

		});

		tf.addKeyListener(new KeyListener() {

			@Override
			public void keyTyped(KeyEvent e) {
				// TODO 自动生成的方法存根

			}

			@Override
			public void keyReleased(KeyEvent e) {
				// TODO 自动生成的方法存根
				if (e.getKeyCode() == KeyEvent.VK_ENTER) {

					String getC2Mstring = tf.getText().trim();
					String[] arg = Server.strsplit(getC2Mstring); // 得到分割后的数据
					for (int i = 0; i < arg.length; i++) {
						String string = arg[i];
						// System.out.println(string);
					}
					if (arg.length > 1) {
						Set<String> nodes = new HashSet<String>();
						nodes.add(po0);
						nodes.add(po1);
						nodes.add(po2);

						ConsistentHash<String> ha = new ConsistentHash<String>(new HashFunction(), 1000, nodes);

						// String
						// s=String.valueOf((int)(1+Math.random()*(1000-1+1)));
						// System.out.println(arg[1]);
						String key = ha.get(arg[1]);
						// System.out.println(key);

						if (key.equals(po0)) {
							try {
								if (arg[0].equals("get") || arg[0].equals("mget")) {
									initClient("localhost", sport0);
									System.out.println("第1台Slave的第" + si + "次连接");
									si++;
								} else {
									initClient("localhost", port0);

									System.out.println("第1台Master的第" + mi + "次连接");
									mi++;
								}

							} catch (IOException e1) {
								// TODO 自动生成的 catch 块
								e1.printStackTrace();
							}
						} else if (key.equals(po1)) {
							try {
								if (arg[0].equals("get") || arg[0].equals("mget")) {
									initClient("localhost", sport1);
									System.out.println("第2台Slave的第" + sj + "次连接");
									sj++;
								} else {
									initClient("localhost", port1);

									System.out.println("第2台Master的第" + mj + "次连接");
									mj++;
								}
							} catch (IOException e1) {
								// TODO 自动生成的 catch 块
								e1.printStackTrace();
							}
						} else if (key.equals(po2)) {
							try {
								if (arg[0].equals("get") || arg[0].equals("mget")) {
									initClient("localhost", sport2);

									System.out.println("第3台Slave的第" + sk + "次连接");
									sk++;
								} else {
									initClient("localhost", port2);

									System.out.println("第3台Master的第" + mk + "次连接");
									mk++;
								}
							} catch (IOException e1) {
								// TODO 自动生成的 catch 块
								e1.printStackTrace();
							}
						} else {
							System.out.println("未成功连接端口");
						}
						sendMsg(tf.getText().trim());
						str = username + ':' + tf.getText().trim() + '\n';
					} else {
						str = username + ':' + tf.getText().trim() + '\n' + "命令错误！"+"\n";

					}
					ta.append(str);
					tf.setText("");
				}

			}

			@Override
			public void keyPressed(KeyEvent e) {
				// TODO 自动生成的方法存根

			}
		});
		jp.add(bq);
		jp.add(tf);
		jp.add(an);
		pack();// 把窗口设置为适合组件大小，不是自己设置的大小
		jf.setVisible(true);

		System.out.println("绑定成功");
		beconnected = true;
	}

	SocketChannel channel;

	// 通道管理器
	private Selector selector;
	// private Unsafe unsafe;

	/**
	 * 获得一个Socket通道，并对该通道做一些初始化的工作
	 * 
	 * @param ip
	 *            连接的服务器的ip
	 * @param port
	 *            连接的服务器的端口号
	 * @throws IOException
	 */
	public void initClient(String ip, int port) throws IOException {

		// 获得一个Socket通道
		channel = SocketChannel.open();
		// 设置通道为非阻塞
		channel.configureBlocking(false);
		// 获得一个通道管理器
		this.selector = Selector.open();
		// 客户端连接服务器,其实方法执行并没有实现连接，需要在listen（）方法中调
		// 用channel.finishConnect();才能完成连接
		channel.connect(new InetSocketAddress(ip, port));
		// 将通道管理器和该通道绑定，并为该通道注册SelectionKey.OP_read事件。
		SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
		beconnected = true;

		new Thread(new ClientThread()).start();

	}

	// 发送消息
	public void sendMsg(String msg) {
		try {
			while (!channel.finishConnect()) {
			}

			channel.write(ByteBuffer.wrap(msg.getBytes()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class ClientThread implements Runnable {
		public void run() {
			try {
				while (beconnected) {
					int readyChannels = selector.select();
					if (readyChannels == 0)
						continue;
					// 可以通过这个方法，知道可用通道的集合
					Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
					while (iter.hasNext()) {
						SelectionKey sk = (SelectionKey) iter.next();
						iter.remove();
						dealWithSelectionKey(sk);
					}
					channel.close();

				}
			} catch (IOException io) {

				try {
					channel.socket().close();
					channel.close();
				} catch (IOException e) {
					// TODO 自动生成的 catch 块
					e.printStackTrace();
				}

			}
		}

		private void dealWithSelectionKey(SelectionKey sk) throws IOException {
			if (sk.isReadable()) {
				// 使用 NIO 读取
				// Channel中的数据，这个和全局变量soketchannel是一样的，因为只注册了一个SocketChannel
				// sc既能写也能读，这边是读
				SocketChannel sc = (SocketChannel) sk.channel();
				int count = 0;
				ByteBuffer buff = ByteBuffer.allocate(1024);
				buff.clear();
				StringBuffer sb = new StringBuffer();
				String content = "";
				while ((count = sc.read(buff)) > 0) {
					sb.append(new String(buff.array(), 0, count));
				}
				if (sb.length() > 0) {
					content = sb.toString();
				}
				System.out.println(content);
				ta.append(content + '\n');
				sk.interestOps(SelectionKey.OP_READ);
			}
		}
	}

}
