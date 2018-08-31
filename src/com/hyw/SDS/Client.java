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
		// TODO �Զ����ɵķ������
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
		// TODO �Զ����ɵĹ��캯�����
		username = name;
		JFrame jf = new JFrame("�ͻ���");
		jf.setLocation(600, 400);
		jf.setSize(600, 500);
		jf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		jf.setResizable(false);

		jf.setLayout(new BorderLayout());
		// ���ڹرռ���
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
		JLabel bq = new JLabel("���������");
		bq.setFont(new java.awt.Font("Dialog", 1, 18));
		JButton an = new JButton("����");
		an.setFont(new java.awt.Font("Dialog", 1, 18));
		// ���Ͱ�ť�����¼�����
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
				// TODO �Զ����ɵķ������

			}

			@Override
			public void keyReleased(KeyEvent e) {
				// TODO �Զ����ɵķ������
				if (e.getKeyCode() == KeyEvent.VK_ENTER) {

					String getC2Mstring = tf.getText().trim();
					String[] arg = Server.strsplit(getC2Mstring); // �õ��ָ�������
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
									System.out.println("��1̨Slave�ĵ�" + si + "������");
									si++;
								} else {
									initClient("localhost", port0);

									System.out.println("��1̨Master�ĵ�" + mi + "������");
									mi++;
								}

							} catch (IOException e1) {
								// TODO �Զ����ɵ� catch ��
								e1.printStackTrace();
							}
						} else if (key.equals(po1)) {
							try {
								if (arg[0].equals("get") || arg[0].equals("mget")) {
									initClient("localhost", sport1);
									System.out.println("��2̨Slave�ĵ�" + sj + "������");
									sj++;
								} else {
									initClient("localhost", port1);

									System.out.println("��2̨Master�ĵ�" + mj + "������");
									mj++;
								}
							} catch (IOException e1) {
								// TODO �Զ����ɵ� catch ��
								e1.printStackTrace();
							}
						} else if (key.equals(po2)) {
							try {
								if (arg[0].equals("get") || arg[0].equals("mget")) {
									initClient("localhost", sport2);

									System.out.println("��3̨Slave�ĵ�" + sk + "������");
									sk++;
								} else {
									initClient("localhost", port2);

									System.out.println("��3̨Master�ĵ�" + mk + "������");
									mk++;
								}
							} catch (IOException e1) {
								// TODO �Զ����ɵ� catch ��
								e1.printStackTrace();
							}
						} else {
							System.out.println("δ�ɹ����Ӷ˿�");
						}
						sendMsg(tf.getText().trim());
						str = username + ':' + tf.getText().trim() + '\n';
					} else {
						str = username + ':' + tf.getText().trim() + '\n' + "�������"+"\n";

					}
					ta.append(str);
					tf.setText("");
				}

			}

			@Override
			public void keyPressed(KeyEvent e) {
				// TODO �Զ����ɵķ������

			}
		});
		jp.add(bq);
		jp.add(tf);
		jp.add(an);
		pack();// �Ѵ�������Ϊ�ʺ������С�������Լ����õĴ�С
		jf.setVisible(true);

		System.out.println("�󶨳ɹ�");
		beconnected = true;
	}

	SocketChannel channel;

	// ͨ��������
	private Selector selector;
	// private Unsafe unsafe;

	/**
	 * ���һ��Socketͨ�������Ը�ͨ����һЩ��ʼ���Ĺ���
	 * 
	 * @param ip
	 *            ���ӵķ�������ip
	 * @param port
	 *            ���ӵķ������Ķ˿ں�
	 * @throws IOException
	 */
	public void initClient(String ip, int port) throws IOException {

		// ���һ��Socketͨ��
		channel = SocketChannel.open();
		// ����ͨ��Ϊ������
		channel.configureBlocking(false);
		// ���һ��ͨ��������
		this.selector = Selector.open();
		// �ͻ������ӷ�����,��ʵ����ִ�в�û��ʵ�����ӣ���Ҫ��listen���������е�
		// ��channel.finishConnect();�����������
		channel.connect(new InetSocketAddress(ip, port));
		// ��ͨ���������͸�ͨ���󶨣���Ϊ��ͨ��ע��SelectionKey.OP_read�¼���
		SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
		beconnected = true;

		new Thread(new ClientThread()).start();

	}

	// ������Ϣ
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
					// ����ͨ�����������֪������ͨ���ļ���
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
					// TODO �Զ����ɵ� catch ��
					e.printStackTrace();
				}

			}
		}

		private void dealWithSelectionKey(SelectionKey sk) throws IOException {
			if (sk.isReadable()) {
				// ʹ�� NIO ��ȡ
				// Channel�е����ݣ������ȫ�ֱ���soketchannel��һ���ģ���Ϊֻע����һ��SocketChannel
				// sc����дҲ�ܶ�������Ƕ�
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
