package cosmosdb.mongo.samples;

import com.google.common.base.Stopwatch;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import cosmosdb.mongo.samples.runnables.InsertDocumentRunnable;
import cosmosdb.mongo.samples.runnables.UpdateOneDocumentRunnable;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RuCharge;
import org.bson.*;
import org.bson.conversions.Bson;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.set;
import static cosmosdb.mongo.samples.InsertionHelper.*;

public class Main {

    private static ConfigSettings configSettings=new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    public static void main(final String[] args) throws Exception {
        configSettings.Init();
        InitMongoClient();
        //RunSimulation();
        //TestRunSimulation();
        FindAndUpdateOneSimulation();


    }

    private static void Run() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        java.lang.reflect.Method method;
        method = Main.class.getDeclaredMethod(configSettings.getScenario());
        method.invoke(null);

    }

    private static void InitMongoClient()
    {
        mongoClientExtension=
                new MongoClientExtension(
                        configSettings.getUserName(),
                        configSettings.getPassword(),
                        10255,
                        true,
                        configSettings.getClientThreadsCount()
                );
    }

    private static void FindAndUpdateOneSimulation()
    {
        String dbName= configSettings.getDbName();
        String collName= configSettings.getCollName();

        BsonDocument findFilter=new BsonDocument();
        findFilter.append("_id.purchaseOrderId",new BsonInt32(1024869703));

        FindIterable<Document> foundDocsIterable= mongoClientExtension.GetClient().getDatabase(dbName).getCollection(collName).find(findFilter);

        List<Document> foundDocs=new ArrayList<>();

        MongoCursor<Document> findIterator=foundDocsIterable.iterator();
        while(findIterator.hasNext())
        {
            foundDocs.add(findIterator.next());
        }

        List<Document> updatedDocs=new ArrayList<>();
        if(foundDocs.size()>0)
        {
            for(int i=0;i<foundDocs.size();i++) {
               updatedDocs.add(UpdateDoc(foundDocs.get(i)));
            }
        }
        for(int j=0;j<updatedDocs.size();j++) {
            mongoClientExtension.GetClient().getDatabase(dbName).getCollection(collName).
                    updateOne(
                            findFilter,
                            combine(
                                    set("poAddtlDistrib", updatedDocs.get(j))
                            ));
        }
    }

    private static Document UpdateDoc(Document doc)
    {
        Document  poAddtlDistribDoc=(Document)doc.get("poAddtlDistrib");

        for(int i=0;i<50;i++) {
            Document child=new Document();
            child.put("origBuId","test2");
            child.put("destBuId","test2");

            poAddtlDistribDoc.put("PoInitialDistribKey [destBuId="+i+"]", child);
        }

        return poAddtlDistribDoc;

    }


    // https://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/perform-write-operations/
    private static void RunSimulation() throws Exception {
        // Get sample documents
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;
        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount);

        // Get collection information
        String dbName= configSettings.getDbName();
        String collName= configSettings.getCollName();
        String partitionKey= configSettings.getPartitionkey();
        int rus=configSettings.getRus();

        String[] indexes=new String[]{"transactionId","vendorNbr","txnSeqNbr"};

        CreateCollectionIfNotExists(dbName,collName,partitionKey,rus,indexes);

        InsertSampleDocuments(sampleDocs);

        List<UpdateOneDocumentRunnable> updateOneDocumentRunnables=
                UpdateOneInParallel(mongoClientExtension,dbName,collName,sampleDocs);
        WaitUntilAllUpdateOneComplete(updateOneDocumentRunnables);
    }

    // https://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/perform-write-operations/
    private static void TestRunSimulation() throws Exception {
        // Get sample documents
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;
        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount);

        // Get collection information
        String dbName= configSettings.getDbName();
        String collName= configSettings.getCollName();
        String partitionKey= configSettings.getPartitionkey();
        int rus=configSettings.getRus();

        String[] indexes=new String[]{"transactionId","vendorNbr","txnSeqNbr"};

        CreateCollectionIfNotExists(dbName,collName,partitionKey,rus,indexes);

        //InsertSampleDocuments(sampleDocs);

        Document sampleUpdate=sampleDocs.get(0);
        sampleUpdate.remove("_id");
        System.out.println(String.format("transaction id: %s, vendorNbr: %s ",sampleUpdate.getString("transactionId"),sampleUpdate.getString("vendorNbr")));
        BsonDocument findFilter=new BsonDocument();
        findFilter.append("transactionId",new BsonString(sampleUpdate.getString("transactionId")));
        findFilter.append("vendorNbr",new BsonString(sampleUpdate.getString("vendorNbr")));

        UpdateOptions options=new UpdateOptions().upsert(true);

        HashMap<String, Object> key1Val=new HashMap<>();
        key1Val.put("processStatusTimestamp","2015-07-01-20.43.45.898783");
        key1Val.put("processStatusCode",1);
        key1Val.put("statusUserId","AP537SN");


        mongoClientExtension.GetClient().getDatabase(dbName).getCollection(collName).
                updateOne(
                        findFilter,
                        combine(
                                set("transactionProcessLogs.{transactionId=614258144, txnSeqNbr=0,processStatusCode=1}", key1Val)
                                ));


        HashMap<String, Object> key1Val2=new HashMap<>();
        key1Val2.put("processStatusTimestamp","2015-07-01-20.43.45.898783");
        key1Val2.put("processStatusCode2",2);
        key1Val2.put("statusUserId","AP537SN");





        mongoClientExtension.GetClient().getDatabase(dbName).getCollection(collName).
                updateOne(
                        findFilter,
                        combine(
                                set("transactionProcessLogs.{transactionId=614258144, txnSeqNbr=0,processStatusCode=2}", key1Val2)
                        ),options);

        RuCharge ruCharge= mongoClientExtension.GetLatestOperationRus(dbName);
        System.out.println("Charge for the recent request: "+ ruCharge.GetRus());
    }



    // db.runCommand({customAction:"createCollection",collection:"test2", shardKey:"_id",offerThroughput:20000})
    private static void CreateCollectionIfNotExists(
            String dbName,
            String collectionName,
            String partitionKey,
            int rus,
            String[] indexes) throws Exception {
        boolean isExits= mongoClientExtension.IsCollectionExists(
                configSettings.getDbName(),
                configSettings.getCollName());
        if(isExits) {
            System.out.println("Collection: " + configSettings.getCollName() +
                    " found in Database: " + configSettings.getDbName());
        }
        else
        {
           if(!partitionKey.equals("")){

               mongoClientExtension.CreatePartitionedCollection(
                      dbName,
                       collectionName,
                       partitionKey,
                       rus,
                       indexes);



           }
           else
           {
               throw new Exception("Please provide valid partition key.");
           }
        }

    }

    private static void InsertSampleDocuments(List<Document> sampleDocs)
            throws URISyntaxException, InterruptedException, IOException {
       List<InsertDocumentRunnable> tasks=InsertOneInParallel(
                mongoClientExtension,
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getPartitionkey(),
                sampleDocs);

        WaitUntilAllInsertionComplete(tasks);

        ValidateInsertResult(tasks,0);

    }

    private static void ValidateInsertResult(
            List<InsertDocumentRunnable> tasks,
            int runId) throws IOException {
        boolean anyFailures=false;
        for(int i=0;i<tasks.size();i++)
        {
            if(!tasks.get(i).GetIsSucceeded())
            {
                anyFailures=true;
            }
        }
        if(anyFailures)
        {
            System.out.println("There are failures while inserting the documents");
            String fileName=String.format("Errors%d.txt",runId);
            File file = new File(fileName);
            FileWriter writer = new FileWriter(file);
            for(int i=0;i<tasks.size();i++)
            {
                if(!tasks.get(i).GetIsSucceeded())
                {
                    writer.write("===Start of failed doc===");
                    writer.write(System.lineSeparator());
                    writer.write(tasks.get(i).GetDocToInsert().toJson());
                    writer.write(System.lineSeparator());
                    writer.write("===Errors===");
                    writer.write(System.lineSeparator());
                    List<String> failedDocsError=tasks.get(i).GetErrorMessages();
                    for(int k=0;k<failedDocsError.size();k++)
                    {
                        writer.write(failedDocsError.get(k));
                        writer.write(System.lineSeparator());
                    }
                    writer.write(System.lineSeparator());
                    writer.write("===Errors End===");
                    writer.write(System.lineSeparator());
                    writer.write("===End Doc===");
                }
            }
            writer.close();
            System.out.println("Failed Document and errors available in file: "+fileName);
        }
        else
        {
            System.out.println("All documents inserted successfully.");
        }
    }

    private static void WaitUntilAllInsertionComplete(List<InsertDocumentRunnable> tasks) throws InterruptedException {
        boolean isCompleted=false;
        final long startTime = System.currentTimeMillis();
        while(!isCompleted)
        {
            isCompleted=true;
            for(int i=0;i<tasks.size();i++)
            {
                if(tasks.get(i).IsRunning())
                {
                    isCompleted=false;
                }
            }
            if(isCompleted)
            {
                Thread.sleep(1);
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);
    }

    private static void WaitUntilAllUpdateOneComplete(List<UpdateOneDocumentRunnable> tasks) throws InterruptedException {
        boolean isCompleted=false;
        final long startTime = System.currentTimeMillis();
        while(!isCompleted)
        {
            isCompleted=true;
            for(int i=0;i<tasks.size();i++)
            {
                if(tasks.get(i).IsRunning())
                {
                    isCompleted=false;
                }
            }
            if(isCompleted)
            {
                Thread.sleep(1);
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Update one docs count: " + tasks.size());
        System.out.println("Update one total docs Execution time in milli seconds: " + totalTime);
        System.out.println("Update one total Execution time in seconds: " + totalTime / 1000);
    }

    public static List<UpdateOneDocumentRunnable> UpdateOneInParallel(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            List<Document> docs)
    {

        int numberOfThreads=docs.size();
        List<UpdateOneDocumentRunnable> threads=new ArrayList<UpdateOneDocumentRunnable>(numberOfThreads);
        for(int i=0;i<numberOfThreads;i++)
        {
            Document sampleUpdate=docs.get(i);
            BsonDocument findFilter=new BsonDocument();
            findFilter.append("transactionId",new BsonString(sampleUpdate.getString("transactionId")));
            findFilter.append("vendorNbr",new BsonString(sampleUpdate.getString("vendorNbr")));
            Bson updateFields=combine(set("invoiceId", 99999999));
           UpdateOneDocumentRunnable updateOneDocumentRunnable= new UpdateOneDocumentRunnable(
                    mongoClientExtension,
                    dbName,
                    collectionName,
                    findFilter,
                   updateFields);
            Thread t = new Thread(updateOneDocumentRunnable);
            threads.add(updateOneDocumentRunnable);
            t.start();
        }

        return threads;
    }
}
