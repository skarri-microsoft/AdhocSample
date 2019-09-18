package cosmosdb.mongo.samples;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.util.*;

public class SampleDoc {

    public static  HashMap<String, Object> Get() throws URISyntaxException {
        String sampleJsonFile=new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent()+"\\classes\\Sample.json";
        System.out.println("Loading sample document "+sampleJsonFile);
        HashMap<String, Object> sampleDocument = new HashMap<String, Object>();
        try {

            //read sample json document
            JSONParser parser = new JSONParser();

            Object obj = parser.parse(new FileReader(sampleJsonFile));
            JSONObject jsonObj = (JSONObject) obj;


            String jsonStr = jsonObj.toJSONString();

            //populate sample doc
            ObjectMapper mapper = new ObjectMapper();
             sampleDocument = mapper.readValue(jsonStr, new TypeReference<Map<String, Object>>(){});
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return sampleDocument;
    }

    public static List<Document> GetSampleDocuments(int count, String partitionKey) throws URISyntaxException {
        HashMap<String, Object> sampleDocument=Get();


        List<Document> documentList=new ArrayList<Document>(count);
        for(int i=0;i<count;i++)
        {
            // TODO Need to move this to config

            String transactionId=UUID.randomUUID().toString();
            HashMap<String, Object> updateIdMap=new HashMap<>();
            updateIdMap.put("txnSeqNbr",0);
            updateIdMap.put("transactionId",transactionId);
            sampleDocument.remove("_id");
            sampleDocument.put("_id",updateIdMap);

            sampleDocument.remove("transactionId");
            sampleDocument.put("transactionId",transactionId);

            sampleDocument.remove("txnSeqNbr");
            sampleDocument.put("txnSeqNbr",0);



            if(!partitionKey.equals("")) {
                String pval = UUID.randomUUID().toString();
                sampleDocument.remove(partitionKey);
                sampleDocument.put(partitionKey, pval);
            }

            Document d = new Document(sampleDocument);
            documentList.add(d);
        }
        return documentList;
    }
}
