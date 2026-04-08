//import com.mongodb.MongoClientSettings;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//
//import java.util.Arrays;
//import java.util.function.Consumer;
//
///**
// * @author Jiangyi Hou
// * @since 19-2-28
// */
//public class Mongo2TaosPipe {
//
//    private static MongoClient mongoClient;
//
//    public static void main(String[] args) {
//
//        Mongo2TaosPipe pipe = new Mongo2TaosPipe();
//        pipe.connect2Mongo();
//        mongoClient.listDatabaseNames().forEach((Consumer<? super String>) System.out::println);
//    }
//
//    private void connect2Mongo() {
////        MongoClient mongoClient1 = MongoClients.create();
////        MongoClient mongoClient2 = MongoClients.create(MongoClientSettings.builder()
////                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017)))).build());
////        MongoClient mongoClient3 = MongoClients.create(MongoClientSettings.builder()
////                .applyConnectionString(new ConnectionString("mongodb://localhost:27017,hostTwo:27018")).build());
////        MongoClient mongoClient4 = MongoClients.create("mongodb://localhost:27017,hostTwo:27018");
//        mongoClient = MongoClients.create(MongoClientSettings.builder()
//                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017)))).build());
//    }
//
//}
