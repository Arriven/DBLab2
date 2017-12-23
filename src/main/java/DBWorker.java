import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;


public class DBWorker {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public void connectToDB() {
        mongoClient = new MongoClient("localhost");
        database = mongoClient.getDatabase("server-logs");
        collection = database.getCollection("logs");
        insertDocumentsToDB();
    }

    public void insertDocumentsToDB(){
        if (collection.count() == 0) {
            ArrayList<Document> listDocs = createJSONlistFromSCV("logs.csv");
            collection.insertMany(listDocs);
        }
    }

    public String convertCSVtoJSON(String csvLine) {
        String[] values = csvLine.split(",");
        StringBuilder json = new StringBuilder();

        json.append("{").append("URL:\"").append(values[0]).append("\",IP:\"")
                .append(values[1]).append("\",timeStamp:\"").append(values[2])
                .append("\",timeSpent:").append(Integer.parseInt(values[3])).append("}");

        return json.toString();
    }

    public ArrayList<Document> createJSONlistFromSCV(String filename) {
        ArrayList<Document> listDocs = new ArrayList<>();
        ArrayList<String> CSVlogs = new ArrayList<>();

        try(Stream<String> stream = Files.lines(Paths.get(filename))){
            stream.forEach(CSVlogs::add);
        }
        catch (IOException e) {
            System.out.println("Cannot read from file.");
        }

        CSVlogs.forEach(csvLog -> {
            Document doc = Document.parse(convertCSVtoJSON(csvLog));
            listDocs.add(doc);
        });

        return listDocs;
    }

    public FindIterable<Document> getSortedIPsByURL(String url) {
        return collection.find(eq("URL", url)).sort(descending("IP")).
                projection(fields(include("IP"), excludeId()));
    }

    public FindIterable<Document> getSortedURLByIP(String ip) {
        return collection.find(eq("IP", ip)).sort(descending("URL")).
                projection(fields(include("URL"), excludeId()));
    }

    public FindIterable<Document> getUrlsSumOfTime() {
        String collectionName = "urlTimeSpend";
        String map = "function (){ emit(this.URL, this.timeSpent); }";
        String reduce = "function(key, values) { return Array.sum(values); }";

        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();

        return database.getCollection(collectionName).find().sort(descending("value"));
    }

    public FindIterable<Document> getUrlByVisitNumber() {
        String collectionName = "urlVisitNumber";
        String map = "function (){ emit(this.URL, 1); }";
        String reduce = "function(key, values) { return Array.sum(values); }";

        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();

        return database.getCollection(collectionName).find().sort(descending("value"));
    }

    public FindIterable<Document> getIPByVisitAndTime() {
        String collectionName = "ipVisitAndTime";
        String map = "function() { emit( this.IP, this.timeSpent); }";
        String reduce = "function(key, values) { return {count:values.length, timeSpent:Array.sum(values)}; }";

        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();

        return database.getCollection(collectionName).find().sort(descending("value"));
    }
}
