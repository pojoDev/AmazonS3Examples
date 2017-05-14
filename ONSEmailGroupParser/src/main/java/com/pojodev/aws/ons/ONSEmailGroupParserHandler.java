package com.pojodev.aws.ons;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

public class ONSEmailGroupParserHandler implements RequestHandler<S3Event, String> {

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event + "\n");

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		try {
			ONSEmailGroupParser parser = new ONSEmailGroupParser();
			context.getLogger().log("Bucket: " + bucket + " key: " + key + "\n");
			parser.parseS3File(context, bucket, key);
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", bucket, key));
			return "Error";
		}
		return "Success";
	}
}