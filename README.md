# AndroidAppsQuery-with-MongoDB-Java
Using java code to drive MongoDB running query in Android apps data of Amazon

Data source url: http://jmcauley.ucsd.edu/data/amazon/, Apps for Android, or other datasets

Step1: running JSONConverter.java to convert the format of the review time in the JSON file.

Step2: running MongoQuery.java to create a new database and a new table in MongoDB, loading the new JSON file that has been created in step1 to the database created, running query in this table to get the result.


There are eight queries, which are listed below:

System.out.println("1: Find top 100K 3* or above rated products");
System.out.println("2: Find reviews with more than 100K number of different reviwers with 1* rating");
System.out.println("3: Find reviews in the range of years 2XXX to 2017");
System.out.println("4: Find reviews where average ratings are above 3* in last X years");
System.out.println("5: Display summaries of top 100k reviews where rating is 1*");
System.out.println("6: Display reviewer IDs of customers who has bought more than 20 products");
System.out.println("7: Find number of products reviewed in a given year");
System.out.println("8: Display top 100K summaries which have key word security");
