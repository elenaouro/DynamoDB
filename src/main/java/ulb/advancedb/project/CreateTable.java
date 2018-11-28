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
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;


public class CreateTable {
	
	public static final String PK = "ID";
	public static final String SK = "Name";
	
	public static final String GSI_1_NAME = "GSI1";
	public static final String GSI_1_SK = "Data";
	
	public static void createTable(DynamoDB dynamoDB, String tableName ) {
	        
        try {
            System.out.println("\nAttempting to create table; please wait...");
            
            GlobalSecondaryIndex GSI = createGSI(dynamoDB);
            
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
            		.withProvisionedThroughput( new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L))
                    .withAttributeDefinitions( Arrays.asList( new AttributeDefinition( PK, ScalarAttributeType.S),
    	                    								  new AttributeDefinition( SK, ScalarAttributeType.S),
    	                                                      new AttributeDefinition( GSI_1_SK, ScalarAttributeType.S)
    	                                                     )
                    						 )
                    .withKeySchema( Arrays.asList( new KeySchemaElement( PK, KeyType.HASH),
                    							   new KeySchemaElement( SK, KeyType.RANGE)
                    							 )
                    			  )
                    .withGlobalSecondaryIndexes(GSI);
        
            Table table = dynamoDB.createTable( createTableRequest );
            table.waitForActive();            
            
            System.out.println( "Success.  Table status: " + table.getDescription().getTableStatus() );
           
        }catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }
	 
	public static GlobalSecondaryIndex createGSI(DynamoDB dynamoDB) {
		
		ProvisionedThroughput ptIndex = new ProvisionedThroughput().withReadCapacityUnits(1L)
	            												   .withWriteCapacityUnits(1L);
	 
		GlobalSecondaryIndex GSI1 = new GlobalSecondaryIndex().withIndexName(GSI_1_NAME)
				.withProvisionedThroughput(ptIndex)
				.withKeySchema( new KeySchemaElement().withAttributeName(SK).withKeyType(KeyType.HASH),//Partition key
								new KeySchemaElement().withAttributeName(GSI_1_SK).withKeyType(KeyType.RANGE) //Sort key
							  )
				.withProjection( new Projection().withProjectionType("ALL"));
	 	
		return GSI1;
	}
}
