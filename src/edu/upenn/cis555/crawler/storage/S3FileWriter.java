package edu.upenn.cis555.crawler.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3FileWriter {
	static File docFile;
	static File urlFile;
	static BufferedWriter docWriter;
	static BufferedWriter urlWriter;
	
	
	public static void setDocFileWriter(File directory) {
		try {
			docFile = new File(directory +"/DocS3batch:" + System.currentTimeMillis() + ".txt");
			docWriter = new BufferedWriter(new FileWriter(docFile));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public static void setUrlFileWriter(File directory) {
		try {
			urlFile = new File(directory +"/UrlS3batch:" + System.currentTimeMillis() + ".txt");
			urlWriter = new BufferedWriter(new FileWriter(urlFile));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static synchronized void writeToDocFile(String line) {
		try {
			docWriter.append(line);
			docWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static synchronized void writeToUrlFile(String line) {
		try {
			urlWriter.append(line);
			urlWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String prepareFileLineDoc(String url, String doc) {
		return url +  "\t" + doc + "CIS555###Split%%%Document***Line";
	}
	
	public static String prepareFileLineUrlList(String url, LinkedList<String> list) {
		return url +  "\t" + list.toString();
	}
	
	
	
	public static void switchFile(File directory) {
	
		synchronized(docWriter) {
			try {
				docWriter.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			setDocFileWriter(directory);
		} 
		
		synchronized(urlWriter) {
			try {
				urlWriter.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			setUrlFileWriter(directory);
		}

	}
}