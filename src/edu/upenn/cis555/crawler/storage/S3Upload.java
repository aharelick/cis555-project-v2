package edu.upenn.cis555.crawler.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Upload {
	private static HashSet<File> files;
	private static File directory;
	private static AmazonS3 s3client;
	private static AWSCredentials credentials;
	private static String bucketName;
	
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: <file_directory> <bucket_name>");
		}
		files = new HashSet<File>();
		// directory containing the files to upload
		directory = new File(args[0]);
		bucketName = args[1];
		if (!directory.exists() || !directory.isDirectory()) {
			System.out.println("The directory specified is not a directory");
			System.exit(0);
		}
		// add all files in the directory provided
		for (File file : directory.listFiles()) {
			if (file.isDirectory()) {
				continue;
			}
			try {
				BasicFileAttributes attr = Files.readAttributes(file.toPath(),
						BasicFileAttributes.class);
				if (System.currentTimeMillis() - attr.lastModifiedTime().toMillis() < 60000) {
					System.out.println("Skipped " + file.toString());
					continue;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			files.add(file);
		}
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/home/cis455/.aws/credentials), and is in valid format.",
							e);
		}
		s3client = new AmazonS3Client(credentials);    
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
		ObjectListing objectListing;
		objectListing = s3client.listObjects(listObjectsRequest);
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			files.remove(new File(directory, objectSummary.getKey()));
		}
		upload();
	}
	
	public static void upload() {
		TransferManager tx = new TransferManager(credentials);
		for (File file : files) {
			System.out.println("Uploading " + file.getName() + "...");
			Upload myUpload = tx.upload(bucketName, file.getName(), file);
			try {
				myUpload.waitForCompletion();
			} catch (AmazonClientException | InterruptedException e) {
				e.printStackTrace();
				continue;
			}
			System.out.println("Finished " + file.getName());
		}
		tx.shutdownNow();

	}
}
