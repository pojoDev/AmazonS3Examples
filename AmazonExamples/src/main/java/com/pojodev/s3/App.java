package com.pojodev.s3;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App {

	AmazonS3 s3;

	public void init(String accessKey, String secretKey) {
		s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
	}

	public void listAllBuckets() throws IOException {
		// List all the buckets
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket next : buckets) {
			String bucketName = next.getName();
			System.out.println("__________________________________________");
			System.out.println("BucketName: " + bucketName);
			this.listBucketContent(bucketName);			
			System.out.println("__________________________________________");
		}
		System.out.println("All bucket listed.");
	}

	public void listBucketContent(String bucketName) throws IOException {
		ObjectListing listing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
			System.out.println(" ------------------------");
			String fileName = objectSummary.getKey();
			long  fileSize = objectSummary.getSize();
			System.out.println("fileName: " + fileName + "  " + "(fileSize= " + fileSize / 1024 + " KB)");
			this.readFromS3(bucketName, fileName);
			
			System.out.println(" ------------------------");
		}
		System.out.println("Uploading UploadIntoS3.txt into S3 bucket: " + bucketName);
		this.uploadFileS3("UploadIntoS3.txt", bucketName, "UploadIntoS3.txt");
	}

	public void readFromS3(String bucketName, String key) throws IOException {
		S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key));
		System.out.println(s3object.getObjectMetadata().getContentType());
		System.out.println(s3object.getObjectMetadata().getContentLength());

		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		while ((line = reader.readLine()) != null) {
			// can copy the content locally as well using a buffered writer
			System.out.println(line);
		}
	}

	public void uploadFileS3(String uploadFileName, String bucketName, String keyName) {
		File file = new File(ClassLoader.getSystemResource(uploadFileName).getFile());;
        this.s3.putObject(new PutObjectRequest(
        		                 bucketName, keyName, file));
        
	}
	public static void main(String[] args) throws IOException {
		App app = new App();
		app.init("accesskey", "secretkey");
		app.listAllBuckets();
	}
}
