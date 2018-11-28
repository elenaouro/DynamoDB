package ulb.advancedb.project;

import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;

import org.json.simple.parser.*;
import org.json.simple.*;

public class LoadData {
	
	public static void load(String path, DynamoDB dynamoDB, String tableName ) {
        
		Table table = dynamoDB.getTable( tableName );
		
        JSONParser parser = new JSONParser();
        JSONArray items = null;
        
        try {
        	items = (JSONArray)parser.parse(new FileReader(path));
        } catch (Exception e) {
            System.err.println("Unable to read the data file");
            System.err.println(e.getMessage());
        }
        
        if( items != null ) {
        	int i = 0;
        	
        	System.out.println("\nLoading data from: "+path+"\n");
        	
        	for (Object o : items) {
        		JSONObject current = (JSONObject) o;
        	
        		Iterator it = current.entrySet().iterator();
        		Item DBItem = new Item();
        		
        		String id = current.get( CreateTable.PK ).toString();
        		String name = current.get( CreateTable.SK ).toString();
        		
        		DBItem.withPrimaryKey( CreateTable.PK, id, CreateTable.SK, name );
        		
	        	for(Object key : current.keySet()) {
	        		
	        		if( !key.toString().equals( CreateTable.PK ) && !key.toString().equals( CreateTable.SK )) {
	        			
	        			if( current.get(key) instanceof String ) {
	        				DBItem.withString( key.toString(), current.get(key).toString() );
	        			} else if ( current.get( key) instanceof Long ) {
	        				DBItem.withLong( key.toString(), (Long)current.get(key) );
	        			} else if ( current.get(key) instanceof Integer ) {
	        				DBItem.withInt( key.toString(), (Integer)current.get(key) );
	        			} else if ( current.get(key) instanceof Double ) {
	        				DBItem.withDouble( key.toString(), (Double)current.get(key) );
	        			}
	        		}
	        	
	        	}

	        	try {
	        		table.putItem(DBItem);
	        		System.out.println("PutItem succeeded: " + id + " " + name);
        		}catch(Exception e) {
        			System.err.println("Unable to add element: ID:"+id+" name:"+name);
        			System.err.println(e.getMessage());
        		}
            }
		}
    }
	
}
