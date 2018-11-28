package ulb.advancedb.project;

import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.json.simple.parser.*;
import org.json.simple.*;

public class LoadData {
	public static void load(String path, DynamoDB dynamoDB) {
        Table table = dynamoDB.getTable("HRTable");
        JSONParser parser = new JSONParser();
        try {
        JSONArray items = (JSONArray)parser.parse(new FileReader(path));
        int i=0;
        for (Object o : items) {
        	JSONObject current = (JSONObject) o;
        	Iterator it = current.entrySet().iterator();
        	Item DBItem = new Item();
        	String ID = current.get("ID").toString();
            String name = current.get("name").toString();
        	DBItem.withPrimaryKey("ID", ID, "name", name);
        	for(Object key : current.keySet()) {
        		if(!key.toString().equals("ID")&&!key.toString().equals("name")) {
        			if(current.get(key) instanceof String) {
        				DBItem.withString(key.toString(), current.get(key).toString());
        			}
        			else if(current.get(key) instanceof Long) {
        				DBItem.withLong(key.toString(), (Long)current.get(key));
        			}
        			else if(current.get(key) instanceof Integer) {
        				DBItem.withInt(key.toString(), (Integer)current.get(key));
        			}
        		}
        	
        	}
        		try {
            	table.putItem(DBItem);
                System.out.println("PutItem succeeded: " + ID + " " + name);
        		}catch(Exception e) {
        			System.err.println("Unable to add element: ID:"+ID+" name:"+name);
        			System.err.println(e.getMessage());
        		}
            }
        }
        catch (Exception e) {
            System.err.println("Unable to read the data file");
            System.err.println(e.getMessage());
        }
    }
}
