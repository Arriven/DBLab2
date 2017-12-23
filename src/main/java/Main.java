import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {
        Metrics.start();
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.SEVERE);

        DBWorker worker = new DBWorker();
        worker.connectToDB();

        mongoQueries(worker);
        mapReduceMongoQueries(worker);
        Metrics.stop();
        Metrics.getResults();
    }

    private static void mongoQueries(DBWorker worker) {
        worker.getSortedIPsByURL("https://docs.oracle.com/");
        worker.getSortedURLByIP("252.73.135.153");
    }

    private static void mapReduceMongoQueries(DBWorker worker) {
        worker.getUrlsSumOfTime();
        worker.getUrlByVisitNumber();
        worker.getIPByVisitAndTime();
    }

}