package com.hyw.SDS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.w3c.dom.Document;

public class demo2 {

	public static void main(String[] args) {
		// TODO 自动生成的方法存根
		new demo2().copyXml("serverdb.xml", "slavedb.xml");
	}
	public void copyXml(String sourcefile,String destfile){
			
		try {
		File source = new File(sourcefile);
		if(source.exists() && source.isFile()){
		File dest=new File(destfile);
		if(dest.exists()){
		dest.delete();
		}
		dest.createNewFile();
		SAXReader saxReader=new SAXReader();
		org.dom4j.Document document=saxReader.read(source);
		System.out.println("dfs");
		
		FileOutputStream outputStream=new FileOutputStream(destfile);
		//漂浪格式，有格式，有空格
		//OutputFormat format=OutputFormat.createPrettyPrint();
		//紧凑格式，无空格换行
		OutputFormat format=OutputFormat.createCompactFormat();
		format.setEncoding("UTF-8");
		XMLWriter writer=new XMLWriter(outputStream, format);
	
		writer.write(document);
		
		writer.close();
		}
		} catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println("xml文件拷贝异常");
		e.printStackTrace();
		}
		}
}
