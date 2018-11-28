package ulb.advancedb.project;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableAlreadyExistsException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import ulb.advancedb.project.LoadData;
import ulb.advancedb.project.CreateTable;

public class Main {

	private static String DBTableName = "HRTable";
	
	public static void main(String[] args) throws Exception {
		if(args.length<1) {
			System.out.println("Usage:\n"
					+ "1: Reset Sample DB\n"
					+ "2: Update item from JSON\n"
					+ "3: Delete item\n"
					+ "4: Add item from JSON\n"
					+ "5: Query the DB");
			return;
		}
		//Connect to the DB
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "local"))
	            .build();
	        DynamoDB dynamoDB = new DynamoDB(client);
		if(!tableExists(DBTableName,dynamoDB)) {
			generateDB(dynamoDB);
		}
		switch(Integer.parseInt(args[0])) {
			case(1):
				resetDB(dynamoDB);
				break;
			case(2):
				
				break;
			case(3):
				break;
			case(4):
				LoadData.load(args[1],dynamoDB);
				break;
			case(5):
				break;
			
		}
	}
	
	private static boolean tableExists(String tableName, DynamoDB dynamoDB) {
        try {
            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            return true;
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException e) {
        	
        }
        return false;

    }
	private static void resetDB(DynamoDB dynamoDB) {
		try {
			Table table = dynamoDB.getTable(DBTableName);
			table.delete();
		}catch(Exception e) {
			e.printStackTrace();
		}
		generateDB(dynamoDB);
		
	}

	private static void generateDB(DynamoDB dynamoDB) {
		CreateTable.createTable(dynamoDB);
		LoadData.load("/home/elena/eclipse-workspace/DynamoDB/src/main/resources/hr.json",dynamoDB);
	}
	
	
}
