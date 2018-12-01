package ulb.advancedb.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;

public class DeleteItem {

	public static void deleteItem(DynamoDB dynamoDB, String tableName, String PK, String SK) {
		Table table = dynamoDB.getTable(tableName);
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec();
        deleteItemSpec.withPrimaryKey(new PrimaryKey("ID", PK, "Name", SK));
		try {
            System.out.println("Deleting item...");
            table.deleteItem(deleteItemSpec);
            System.out.println("DeleteItem succeeded");
        }
        catch (Exception e) {
            System.err.println("Unable to delete item: " + PK + " " + SK);
            System.err.println(e.getMessage());
        }
	}
}
