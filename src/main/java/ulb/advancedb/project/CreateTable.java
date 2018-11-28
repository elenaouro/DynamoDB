package ulb.advancedb.project;

import java.util.Arrays;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;


public class CreateTable {
	
	 public static void createTable(DynamoDB dynamoDB) {
	        String tableName = "HRTable";
	        try {
	            System.out.println("Attempting to create table; please wait...");
	            GlobalSecondaryIndex GSI = createGSI(dynamoDB);
	            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
	                    .withProvisionedThroughput(
	                        new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L))
	                    .withAttributeDefinitions(Arrays.asList(new AttributeDefinition("ID", ScalarAttributeType.S),
	    	                    new AttributeDefinition("name", ScalarAttributeType.S),
	    	                    new AttributeDefinition("Data",ScalarAttributeType.S)))
	                    		.withKeySchema(Arrays.asList(new KeySchemaElement("ID", KeyType.HASH),
	                    				new KeySchemaElement("name", KeyType.RANGE)))
	                    .withGlobalSecondaryIndexes(GSI);
	            Table table = dynamoDB.createTable(createTableRequest);
	            table.waitForActive();
	            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
	           
	        }
	        catch (Exception e) {
	            System.err.println("Unable to create table: ");
	            System.err.println(e.getMessage());
	        }
	    }
	 
	 public static GlobalSecondaryIndex createGSI(DynamoDB dynamoDB) {
		 ProvisionedThroughput ptIndex = new ProvisionedThroughput().withReadCapacityUnits(1L)
		            .withWriteCapacityUnits(1L);
		 GlobalSecondaryIndex GSI1 = new GlobalSecondaryIndex().withIndexName("GSI1")
		            .withProvisionedThroughput(ptIndex)
		            .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH),//Partition key
		            		new KeySchemaElement().withAttributeName("Data").withKeyType(KeyType.RANGE)) //Sort key
		            .withProjection(
		                new Projection().withProjectionType("ALL"));
		 	return GSI1;
	 }
}
