package cosmosdb.mongo.samples.runnables;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.result.UpdateResult;
import cosmosdb.mongo.samples.ErrorHandler;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class UpdateOneDocumentRunnable implements Runnable {

    private long id;
    private Bson updateFields;
    private BsonDocument findFilter;
    private List<String> ErrorMessages = new ArrayList<>();
    private int defaultRetriesForThrottles = 3;
    private String dbName;
    private String collectionName;
    private MongoClientExtension mongoClientExtension;
    private int MaxWaitInMilliSeconds = 500;
    private int MinWaitInMilliSeconds = 100;
    private boolean isThrottled = false;
    private boolean isSucceeded = false;
    private boolean isRunning = true;

    public UpdateOneDocumentRunnable(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            BsonDocument findFilter,
            Bson updateFields) {
        this.mongoClientExtension = mongoClientExtension;
        this.findFilter=findFilter;
        this.updateFields = updateFields;
        this.dbName = dbName;
        this.collectionName = collectionName;

    }

    public Bson GetUpdateFields() {

        return updateFields;
    }

    public BsonDocument GetFindFilter() {

        return findFilter;
    }

    public boolean IsRunning() {
        return this.isRunning;
    }

    public boolean GetIsSucceeded() {
        return this.isSucceeded;
    }

    public List<String> GetErrorMessages() {
        return this.ErrorMessages;
    }

    private void ExecuteUpdateOne(BsonDocument findFilter,Bson updateFields) throws InterruptedException {
        int throttleRetries=0;

        while(throttleRetries<defaultRetriesForThrottles)
        {
            try
            {
                UpdateResult updateResult=this.mongoClientExtension.UpdateOne(this.dbName,this.collectionName,findFilter,updateFields);
                if(updateResult.getMatchedCount()<1)
                {
                    // Requires retry since the document is not propagated.
                    isThrottled=true;
                }
                else {
                    System.out.println("Matched count :" + updateResult.getMatchedCount());
                    isSucceeded = true;
                    break;
                }
            }
            catch (MongoCommandException mongoCommandException)
            {
                if(ErrorHandler.IsThrottle(mongoCommandException))
                {
                    throttleRetries++;
                    isThrottled=true;

                }
                else
                {
                    ErrorMessages.add(mongoCommandException.toString());
                    break;
                }
            }
            catch (Exception ex)
            {
                if(ErrorHandler.IsThrottle(ex.getMessage()))
                {
                    throttleRetries++;
                    isThrottled=true;
                }
                else
                {
                    ErrorMessages.add(ex.getMessage());
                    break;
                }
            }
            if(isThrottled)
            {
                System.out.print("Throttled on thread id: "+id);
                Thread.sleep(new Random().nextInt(MaxWaitInMilliSeconds - MinWaitInMilliSeconds + 1) + MinWaitInMilliSeconds);
            }
        }
    }

    public void run() {

        this.id = Thread.currentThread().getId();

            try {
                ExecuteUpdateOne(this.findFilter,this.updateFields);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        this.isRunning=false;
    }


}
