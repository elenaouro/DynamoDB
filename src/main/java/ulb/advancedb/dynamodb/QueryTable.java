package ulb.advancedb.dynamodb;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class QueryTable {

	public static void filterPKAndSK( DynamoDB dynamoDB, String tableName, String pkQuery, String skQuery ) {
		
		Table table = dynamoDB.getTable( tableName );
		
		ItemCollection<ScanOutcome> items = table.scan( new ScanFilter( CreateTable.PK ).beginsWith(pkQuery), new ScanFilter( CreateTable.SK ).beginsWith(skQuery) );
        
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toJSON());
        }
		
	}
	
	public static void queryEmployeeID( DynamoDB dynamoDB, String tableName, String employeeId ) {
		
		Table table = dynamoDB.getTable( tableName );
		
		ItemCollection<QueryOutcome> items = table.query( new KeyAttribute( CreateTable.PK, "HR-"+employeeId ) );
				//table.scan( new ScanFilter( CreateTable.PK ).beginsWith(pkQuery) );
        
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            
            System.out.println(item.toJSON());
        }
		
	}
	
	public static void queryEmployeeName( DynamoDB dynamoDB, String tableName, String employeeName ) {
	

		Table table = dynamoDB.getTable( tableName );
		
		ItemCollection<ScanOutcome> items = table.scan( new ScanFilter( CreateTable.PK ).beginsWith("HR-EMPLOYEE"), new ScanFilter( "Data" ).eq(employeeName) );
        
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toJSON());
        }
        
	}
	
	public static void queryEmployeesHiredReccently( DynamoDB dynamoDB, String tableName, Date date ) {
		

		Table table = dynamoDB.getTable( tableName );
		
		Index index = table.getIndex(CreateTable.GSI_1_NAME);
		
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("#name = :v_id and #data > :v_hired_date")
				.withNameMap( new NameMap().with("#name", CreateTable.SK)
										   .with("#data", CreateTable.GSI_1_SK ) ) 
			    .withValueMap(new ValueMap().withString(":v_id", "HR-CONFIDENTIAL")
			    							.withLong(":v_hired_date", date.getTime() ) )
			    .withConsistentRead(true);
		
        
		ItemCollection<QueryOutcome> items = index.query(spec);
		
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toJSON());
        }
        
	}
	
	public static void queryJobId( DynamoDB dynamoDB, String tableName, String employeeId, String jobId ) {
		

		Table table = dynamoDB.getTable( tableName );
		
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("#id = :v_id and #name = :v_job_id")
				.withNameMap( new NameMap().with("#id", CreateTable.PK)
										   .with("#name", CreateTable.SK ) ) 
			    .withValueMap(new ValueMap().withString(":v_id", "HR-"+employeeId)
			    							.withString(":v_job_id", jobId) )
			    .withConsistentRead(true);
		
		ItemCollection<QueryOutcome> items = table.query(spec);
		
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toJSON());
        }
        
	}
	
	public static void queryEmployeeCurrentJob( DynamoDB dynamoDB, String tableName, String employeeId ) {
		

		Table table = dynamoDB.getTable( tableName );
		
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("#id = :v_id and #name = :v_emp_id")
				.withNameMap( new NameMap().with("#id", CreateTable.PK)
										   .with("#name", CreateTable.SK ) ) 
			    .withValueMap(new ValueMap().withString(":v_id", "HR-"+employeeId)
			    							.withString(":v_emp_id", employeeId) )
			    .withConsistentRead(true);
		
		ItemCollection<QueryOutcome> items = table.query( spec );
        
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            
            String jobId = (String) item.get("JobId");
            
            queryJobId( dynamoDB, tableName, employeeId, jobId );
            
            //System.out.println(item.toJSON());
        }
        
	}
	
	public static void queryEmployeeWithJobID( DynamoDB dynamoDB, String tableName, String jobID ) {
		

		Table table = dynamoDB.getTable( tableName );
		
		Index index = table.getIndex(CreateTable.GSI_1_NAME);
		
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("#name = :v_jobID ")
				.withNameMap( new NameMap().with("#name", CreateTable.SK) ) 
			    .withValueMap(new ValueMap().withString(":v_jobID", jobID ))
			    .withConsistentRead(true);
		
		ItemCollection<QueryOutcome> items = index.query(spec);
		
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toJSON());
        }
        
	}
	
	public static void filterPK( DynamoDB dynamoDB, String tableName, String pkQuery ) {
		
		Table table = dynamoDB.getTable( tableName );
        
		ItemCollection<ScanOutcome> items = table.scan( new ScanFilter( CreateTable.PK ).beginsWith(pkQuery) );
        
        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            
            System.out.println(item.toJSON());
        }
		
	}
	
}
