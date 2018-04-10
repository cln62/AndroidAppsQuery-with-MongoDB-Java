import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

public class MongoQuery {

    public static void main( String args[] ) throws IOException, JSONException {


        BufferedReader br = new BufferedReader(new FileReader(
                "directory of the new json file you saved"));

        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

        MongoDatabase mongoDatabase = mongoClient.getDatabase("mongo");
        System.out.println("Database created successfully!");
        mongoDatabase.createCollection("query");
        System.out.println("Collection created successfully!");

        List<DBObject> list = new ArrayList<DBObject>();

        String line = br.readLine();

        while (line != null) {
            JSONObject dataJson = new JSONObject(line);
            DBObject dbo = (DBObject) com.mongodb.util.JSON.parse(dataJson.toString());
            list.add(dbo);
            line = br.readLine();
        }

        br.close();

        new MongoClient().getDB("mongo").getCollection("query").insert(list);

        MongoCollection collection = mongoDatabase.getCollection("query");

        Block<Document> printBlock = new Block<Document>() {

            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        while(true) {
            System.out.println("1: Find top 100K 3* or above rated products");
            System.out.println("2: Find reviews with more than 100K number of different reviwers with 1* rating");
            System.out.println("3: Find reviews in the range of years 2XXX to 2017");
            System.out.println("4: Find reviews where average ratings are above 3* in last X years");
            System.out.println("5: Display summaries of top 100k reviews where rating is 1*");
            System.out.println("6: Display reviewer IDs of customers who has bought more than 20 products");
            System.out.println("7: Find number of products reviewed in a given year");
            System.out.println("8: Display top 100K summaries which have key word security");
            System.out.println("0: exit program");

            System.out.println("Select a query from the below list to execute : ");
            Scanner sc = new Scanner(System.in);
            int option = sc.nextInt();

            if (option == 0) {
                System.out.println("Query completed, bye");
                break;
            }

            if (option == 1) {
                System.out.println("Find top 100K 3* or above rated products: ");
                BasicDBObject query = new BasicDBObject("overall",new BasicDBObject("$gte", 3.0));
                FindIterable result = collection.find(query).projection(fields(include("asin", "overall"), excludeId())).limit(100000);
                MongoCursor cursor = result.iterator();
                while(cursor.hasNext()) {
                    System.out.println(cursor.next());
                }
            }

            if (option == 2) {
                System.out.println("Find reviews with more than 100K number of different reviwers with 1* rating :");

                collection.aggregate(
                        Arrays.asList(
                                Aggregates.match(Filters.eq("overall", 1.0)),
                                Aggregates.group("$asin", Accumulators.sum("count", 1)),
                                Aggregates.match(Filters.eq("count", new BasicDBObject("$gte", 100000)))
                        )
                ).forEach(printBlock);

            }

            if (option == 3) {
                System.out.println("Find reviews in the range of years 2XXX to 2017");
                Scanner sc2 = new Scanner(System.in);
                System.out.println("Enter year from when you wish to fetch the reviews:");
                int yr = Integer.valueOf(String.valueOf(sc2.nextInt()) + "0101");
                BasicDBObject query = new BasicDBObject("reviewTime", new BasicDBObject("$gte", yr).append("$lte", 20171231));
                FindIterable result = collection.find(query).projection(fields(include("reviewText", "reviewTime"), excludeId()));
                MongoCursor cursor = result.iterator();
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toString());
                }
            }

            if (option == 4) {
                System.out.println("Find reviews where average ratings are above 3* in last X years");
                Scanner sc3 = new Scanner(System.in);
                System.out.println("Enter last X year(s) from when you wish to fetch the reviews : ");
                Calendar date = Calendar.getInstance();
                int curYear = date.get(Calendar.YEAR);
                int beginDate = Integer.valueOf(String.valueOf(curYear - sc3.nextInt()) + "0101");
                String ey = String.valueOf(curYear);
                String ed = String.valueOf(date.get(Calendar.DATE));
                if (ed.length() < 2) {
                    ed = '0' + ed;
                }
                String em = String.valueOf(date.get(Calendar.MONTH));
                if (em.length() < 2) {
                    em = '0' + em;
                }
                int endDate = Integer.valueOf(ey + em + ed);
                BasicDBObject query = new BasicDBObject("reviewTime", new BasicDBObject("$gte", beginDate)
                        .append("$lte", endDate))
                        .append("overall", new BasicDBObject("$gte", 3.0));
                FindIterable result = collection.find(query).projection(fields(include("reviewText", "reviewTime", "overall"), excludeId()));
                MongoCursor cursor = result.iterator();
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toString());
                }
            }

            if (option == 5) {
                System.out.println("Display summaries of top 100k reviews where rating is 1*");
                BasicDBObject query = new BasicDBObject("overall",1.0);
                FindIterable result = collection.find(query).projection(fields(include("summary", "overall"), excludeId())).limit(100000);
                MongoCursor cursor = result.iterator();
                while(cursor.hasNext()) {
                    System.out.println(cursor.next());
                }
            }

            if (option == 6) {
                System.out.println("Display reviewer IDs of customers who has bought more than 20 products");
                collection.aggregate(
                        Arrays.asList(
                                //Aggregates.match(Filters.eq("overall", 1.0)),
                                Aggregates.group("$reviewerID", Accumulators.sum("count", 1)),
                                Aggregates.match(Filters.eq("count", new BasicDBObject("$gte", 20)))
                        )
                ).forEach(printBlock);
            }

            if (option == 7) {
                System.out.println("Find number of products reviewed in a given year");
                System.out.println("Enter year from when you wish to fetch the list of products:");
                Scanner sc4 = new Scanner(System.in);
                int yr = sc4.nextInt();
                int beginDate = Integer.valueOf(String.valueOf(yr) + "0101");
                int endDate = Integer.valueOf(String.valueOf(yr) + "1231");
                int count = 0;
                Iterable<Document> iter = collection.aggregate(
                        Arrays.asList(
                                Aggregates.match(Filters.eq("reviewTime", new BasicDBObject("$gte", beginDate).append("$lte", endDate))),
                                Aggregates.group("$asin", Accumulators.sum("count", 1))
                        )
                );
                for (Document doc : iter) {
                    System.out.println(doc.toJson());
                    count++;
                }
                System.out.println("Total number of products in the given year is " + count);
            }

            if (option == 8) {
                System.out.println("Display top 100K summaries which have key word security");
                BasicDBObject query = new BasicDBObject("summary", new BasicDBObject("$regex", "security"));
                FindIterable result = collection.find(query).projection(fields(include("summary", "asin"), excludeId())).limit(100000);
                MongoCursor cursor = result.iterator();
                while(cursor.hasNext()) {
                    System.out.println(cursor.next());
                }
            }

            System.out.println("Press 9 to continue");
            Scanner sc5 = new Scanner(System.in);
            int cont = sc5.nextInt();
            if (cont == 9) {
                continue;
            }
            else {
                System.out.println("Invalid input!");
                break;
            }
        }


    }


}