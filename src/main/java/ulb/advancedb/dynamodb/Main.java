package ulb.advancedb.dynamodb;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableAlreadyExistsException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import ulb.advancedb.dynamodb.CreateTable;
import ulb.advancedb.dynamodb.LoadData;

import com.amazonaws.services.dynamodbv2.document.Item;

public class Main {

	private static String DBTableName = "HRTable";
	
	
	public static void main(String[] args) throws Exception {
		
		if( args.length < 1 ) {
			
			System.out.println("Usage:\n"
					+ "1: Reset Sample DB\n"
					+ "2: Update item from JSON. Parameters: 1.Path to JSON file containing the updates\n"
					+ "3: Delete item. Parameters: 1.Partition key 2.Sort key \n"
					+ "4: Add item from JSON. Parameters: 1.Path to JSON file\n"
					+ "5: Query the DB. Parameters: 1.number of query 2.Parameters required by specified query:\n"
					+ "		-1: Look up employee details by employee ID. Parameters: 1.Employee ID\n"
					+ "		-2: Query employee details by employee name. Parameters: 1.Employee name\n"
					+ "		-3: Get an employee's current job details only. Parameters: 1.Employee ID\n"
					+ "		-4: All employees hired recently. Parameters: 1.Emloyee ID 2.Oldest date of hiring to be retrieved YYYY-MM-DD\n"
					+ "		-5: Get all employees with a specific job title. Parameters: 1.Job title\n");
			return;
		}
		
		//Connect to the DB
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	            				.withEndpointConfiguration(	new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "local"))
	            				.build();
		
		DynamoDB dynamoDB = new DynamoDB(client);
	    
	    boolean stateZero = false;
	    
		if( !tableExists( DBTableName,dynamoDB ) ) {
			
			generateDB(dynamoDB);
			stateZero = true;
			
		}
		
		switch( Integer.parseInt( args[0] ) ) {
			case 1:
					if(!stateZero)
						resetDB(dynamoDB);
					
				break;
					
			case 2:
				
				if(args.length>1) UpdateItem.updateItem(dynamoDB, DBTableName, args[1]);
				else {
					System.out.println("Usage: Update item from JSON. Parameters: 1.Path to JSON file containing the updates");
				}
				break;
				
			case 3 :
				
				if(args.length>2) DeleteItem.deleteItem(dynamoDB, DBTableName, args[1], args[2]);
				else {
					System.out.println("Usage: Delete item. Parameters: 1.Partition key 2.Sort key");
				}
				break;
				
			case 4:
			
				if(args.length>1) LoadData.load( args[1], dynamoDB, DBTableName );
				else {
					System.out.println("Usage: Add item from JSON. Parameters: 1.Path to JSON file");
				}
				break;
				
			case 5:
				
				if( args.length>1 ) {
					
					switch( Integer.parseInt( args[1] ) ) {
						case 1:
							
							if(args.length>2) QueryTable.queryEmployeeID(dynamoDB, DBTableName, args[2] );
							else System.out.println("Look up employee details by employee ID. Parameters: 1.Employee ID");
							break;
						case 2:
							if(args.length>2) QueryTable.queryEmployeeName(dynamoDB, DBTableName, args[2]);
							else System.out.println("Look up employee details by employee ID. Parameters: 1.Employee name");
							break;
						case 3:
							if(args.length>2) QueryTable.queryEmployeeCurrentJob(dynamoDB, DBTableName, args[2]);
							else System.out.println("Look up employee details by employee ID. Parameters: 1.Employee ID");
							break;
						case 4:
							SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
							if(args.length>2) QueryTable.queryEmployeesHiredReccently(dynamoDB, DBTableName, format.parse( args[2] ));
							else System.out.println("Look up employee details by employee ID. Parameters: 1.Oldest date of hiring to be retrieved yyyy-MM-dd");
							break;
						case 5:
							if(args.length>2) QueryTable.queryEmployeeWithJobID(dynamoDB, DBTableName, args[2]);
							else System.out.println("Look up employee details by employee ID. Parameters: 1.Job title");
							break;
					}

				}else {
					System.out.println("Usage: 5: Query the DB. Parameters: 1.number of query 2.Parameters required by specified query:\n"
							+ "		-1: Look up employee details by employee ID. Parameters: 1.Employee ID\n"
							+ "		-2: Query employee details by employee name. Parameters: 1.Employee name\n"
							+ "		-3: Get an employee's current job details only. Parameters: 1.Employee ID\n"
							+ "		-4: All employees hired recently. 1.Oldest date of hiring to be retrieved yyyy-MM-dd\n"
							+ "		-5: Get all employees with a specific job title. Parameters: 1.Job title\n" );
				}

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
		
		CreateTable.createTable(dynamoDB, DBTableName);
		LoadData.load("src/main/resources/Employee.json",dynamoDB, DBTableName);
		
	}
	
	
	
	
}
