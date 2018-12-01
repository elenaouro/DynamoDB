package ulb.advancedb.dynamodb;

import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

public class UpdateItem {

	public static void updateItem(DynamoDB dynamodb, String tableName, String path) {
		Table table = dynamodb.getTable(tableName);
		UpdateItemSpec updateItemSpec = new UpdateItemSpec();
		//PArse file
		JSONParser parser = new JSONParser();
        JSONObject item = null;
        
        try {
        	item = (JSONObject)parser.parse(new FileReader(path));
        } catch (Exception e) {
            System.err.println("Unable to read the data file");
            e.printStackTrace();
        }
        String PK;
        String SK;
        if(item!=null) {
        	PK=item.get("ID").toString();
        	SK=item.get("Name").toString();
        	int numKeys=1;
			updateItemSpec.withPrimaryKey("ID",PK,"Name",SK);
			++numKeys;
			String updateExpression = "set ";
			String attributeKey = ":a";
			Integer attributeIndex = 1;
			ValueMap map = new ValueMap();
	        for(Object key : item.keySet()) {
	        	if(!key.toString().equals("ID")&&!key.toString().equals("Name")) {
	        		updateExpression += key.toString()+" = "+attributeKey+attributeIndex.toString();
	        		if(attributeIndex<(item.keySet().size()-numKeys))updateExpression+=", ";
	        		if( item.get(key) instanceof String ) {
        				map.withString(attributeKey+attributeIndex.toString(), item.get(key).toString());
        			} else if ( item.get( key) instanceof Long ) {
        				map.withNumber(attributeKey+attributeIndex.toString(), (Long)item.get(key));
        			} else if ( item.get(key) instanceof Integer ) {
        				map.withNumber(attributeKey+attributeIndex.toString(), (Long)item.get(key));
        			} else if ( item.get(key) instanceof Double ) {
        				map.withString(attributeKey+attributeIndex.toString(), item.get(key).toString());
        			}
	        		++attributeIndex;
	        	}
	        }
	        updateItemSpec.withUpdateExpression(updateExpression)
            .withValueMap(map)
            .withReturnValues(ReturnValue.UPDATED_NEW);
        	try {
                System.out.println("Updating the item...");
                UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
                System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

            }
            catch (Exception e) {
                System.err.println("Unable to update item: " + PK + " " + SK);
                System.err.println(e.getMessage());
            }
	    }
	}
}
