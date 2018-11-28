package ulb.advancedb.dynamodb;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

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
