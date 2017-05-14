package com.pojodev.aws.ons;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class ONSEmailGroupParser {

	Map<String, Set<String>> groupIdMap = new HashMap<String, Set<String>>();

	public void parseS3File(Context context, String bucketName, String key) throws IOException {
		
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

		S3Object s3object = s3Client.getObject(bucketName, key);
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String record;
		while ((record = reader.readLine()) != null) {
			this.parseRecord(record);
		}
		context.getLogger().log("Print Map \n");
		this.printMap(s3Client, bucketName, context);
		context.getLogger().log("Write Map into files \n");
		this.writeMapIntoFiles();
		context.getLogger().log("Upload files into S3 bucket \n");
		this.uploadFileS3(s3Client, bucketName);
	}

	private void parseRecord(String record) {
		String[] recordSplitter = record.split("~");
		String emailAddress = recordSplitter[0];
		String groupName = recordSplitter[1];
		this.insertInMap(emailAddress, groupName);
	}

	private void insertInMap(String emailAddress, String groupName) {
		Set<String> emailAddressSet = this.groupIdMap.get(groupName);
		if (emailAddressSet == null) {
			emailAddressSet = new HashSet<String>();
			emailAddressSet.add(emailAddress);
			this.groupIdMap.put(groupName, emailAddressSet);
		} else {
			emailAddressSet.add(emailAddress);
		}
	}

	private void writeMapIntoFiles() {
		for (String groupName : this.groupIdMap.keySet()) {
			try {
				String fileName = groupName.replace(" ", "_");
				fileName = groupName.replace("/", "_");
				fileName = "/tmp/" + fileName;
				Path file = Paths.get(fileName);
				Files.write(file, this.groupIdMap.get(groupName), Charset.forName("UTF-8"));
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	private void uploadFileS3(AmazonS3 s3Client, String bucketName) {
		for (String groupName : this.groupIdMap.keySet()) {
			String fileName = groupName.replace(" ", "_");
			fileName = groupName.replace("/", "_");
			fileName = "/tmp/" + fileName;
			File file = new File(fileName);
			s3Client.putObject(new PutObjectRequest(bucketName, fileName, file));
		}
	}
	
	private void printMap(AmazonS3 s3Client, String bucketName, Context context) {
		for (String groupName : this.groupIdMap.keySet()) {
			String fileName = groupName.replace(" ", "_");
			fileName = groupName.replace("/", "_");
			
			int noOfContacts = this.groupIdMap.get(groupName).size();
			context.getLogger().log("File Name: " + fileName + " No Of Contacts: " + noOfContacts);
		}
	}
}
