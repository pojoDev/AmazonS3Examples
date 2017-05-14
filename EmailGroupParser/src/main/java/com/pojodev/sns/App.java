package com.pojodev.sns;

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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class App {

	AmazonS3 s3;
	Map<String, Set<String>> groupIdMap = new HashMap<String, Set<String>>();

	public void init(String accessKey, String secretKey) {
		s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
	}

	public void parseS3File(String bucketName, String key) throws IOException {

		S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key));
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		while ((line = reader.readLine()) != null) {
			this.parseRecord(line);
		}
		this.writeIntoFiles();
		this.uploadFileS3(bucketName);
	}

	private void parseRecord(String record) {
		String[] lineSplit = record.split("~");
		String emailAddress = lineSplit[0];
		String groupName = lineSplit[1];
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

	private void writeIntoFiles() {
		for (String groupName : this.groupIdMap.keySet()) {
			try {
				String fileName = groupName.replace(" ", "_");
				Path file = Paths.get(fileName);
				Files.write(file, this.groupIdMap.get(groupName), Charset.forName("UTF-8"));
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	public void uploadFileS3(String bucketName) {
		
		for (String groupName : this.groupIdMap.keySet()) {
			String fileName = groupName.replace(" ", "_");
			File file = new File(fileName);
			this.s3.putObject(new PutObjectRequest(bucketName, fileName+".txt", file));
		}
	}

	public static void main(String[] args) throws IOException {
		App app = new App();
		app.init("AKIAIMZLEVWWZGTJBMAQ", "7He8fQDFJMhNK7sRHgjN+LgXLQfjNxWUSL26RBQk");
		app.parseS3File("pojodevbucket0000", "EmailGroup.txt");
	}
}
