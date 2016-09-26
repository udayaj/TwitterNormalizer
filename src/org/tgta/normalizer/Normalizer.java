package org.tgta.normalizer;

import java.util.Date;
import org.apache.commons.daemon.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import org.apache.commons.configuration.*;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author udaya
 */
public class Normalizer implements Daemon{

    private static String DB_HOST = "";
    private static String DB_PORT = "";
    private static String DB_USER = "";
    private static String DB_PASSWORD = "";
    private static String DB_NAME = "";
    private static int ROWS_READ = 2000;
    private static Connection db = null;

    private static int delay = 2000;//3 seconds
    private static boolean sleep = true;

    public static void main(String[] args) {
        System.out.println("Normalizing started at " + new Date().toString());

        //Start Word Normalizer
        WordNormalizer wn = new WordNormalizer();
        wn.init();

        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);

            DB_HOST = config.getString("database.host");
            DB_PORT = config.getString("database.port");
            DB_USER = config.getString("database.user");
            DB_PASSWORD = config.getString("database.password");
            DB_NAME = config.getString("database.name");
            ROWS_READ = config.getInt("tweet-tagger.rows-read", 2000);

            System.out.println("DB_HOST: " + DB_HOST);
            System.out.println("DB_PORT: " + DB_PORT);
            System.out.println("DB_USER: " + DB_USER);
            System.out.println("DB_PASSWORD: " + DB_PASSWORD);
            System.out.println("DB_NAME: " + DB_NAME);

            //XMLConfiguration config = new XMLConfiguration("config\\config.xml");
            //System.out.println("consumer key: " + config.getString("database.url"));     
            Class.forName("org.postgresql.Driver");
            String dbUrl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;

            db = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
            //System.exit(0);
        } catch (org.apache.commons.configuration.ConfigurationException cex) {
            System.err.println("Error occurred while reading configurations....");
            System.err.println(cex);
            System.exit(0);
        } catch (ClassNotFoundException ex) {
            System.err.println("Database Driver not found....");
            System.err.println(ex);
            System.exit(0);
        } catch (SQLException ex) {
            System.err.println("Database Connection failed....");
            System.err.println(ex);
            System.exit(0);
        }

        while (true) {
            sleep = true;

            try {
                String tweetsQuery = "SELECT * FROM public.\"Message\" WHERE (\"IsNormalized\" IS NULL OR  \"IsNormalized\" = false) ORDER BY \"Id\" ASC LIMIT " + ROWS_READ + ";";

                Statement st = db.createStatement();
                ResultSet rs = st.executeQuery(tweetsQuery);

                while (rs.next()) {
                    sleep = false;

                    RawTweet tweet = new RawTweet();
                    tweet.Id = rs.getLong(1);
                    tweet.TwitterId = rs.getLong(2);
                    tweet.MsgText = rs.getString(3);
                    tweet.Source = rs.getString(4);
                    tweet.Longitude = rs.getFloat(5);
                    tweet.Latitude = rs.getFloat(6);
                    tweet.CreatedTime = new Date(rs.getTimestamp(7).getTime());
                    tweet.PlaceCountry = rs.getString(8);
                    tweet.PlaceCountryCode = rs.getString(9);
                    tweet.PlaceTwitterId = rs.getString(10);
                    tweet.PlaceName = rs.getString(11);
                    tweet.PlaceType = rs.getString(12);
                    tweet.PlacePolygon = rs.getString(13);
                    tweet.RetweetCount = rs.getInt(14);
                    tweet.UserLocation = rs.getString(15);
                    tweet.PlaceFullName = rs.getString(16);
                    tweet.IsNormalized = rs.getBoolean(17);
                    tweet.IsProcessed = rs.getBoolean(18);


                    String cleanedText = ArkTweetCleaner.getCleanedText(tweet.MsgText);                    
                    
                    //cleanedText = cleanedText.replaceAll("[\\s]{2,}", " ");
                    String[] arrStr = cleanedText.split(" ");

                    //Standardize words
                    if (arrStr != null && arrStr.length > 0) {
                        for (int i = 0; i < arrStr.length; i++) {
                            String word = wn.getWord(arrStr[i]);
                            arrStr[i] = (word == null) ? arrStr[i] : word;
                        }
                        cleanedText = StringUtils.join(arrStr, " ");
                        cleanedText = cleanedText.trim();
                        
                        if(cleanedText.length() > 250)
                            cleanedText = cleanedText.substring(0, 250);
                        
                        tweet.MsgText = cleanedText;
                    }
                    
                    //System.out.println(cleanedText + "\n");
                    Normalizer.SaveNormalizedTweet(tweet);
                    Normalizer.UpdateOriginalTweet(tweet);
                }                

            } catch (SQLException ex) {
                System.err.println("Error in SQL Query execution ....");
                System.err.println(ex);
                System.exit(0);
            }

            if (sleep == true) {
                try {
                    //System.out.println("Going to sleep........................");
                    Thread.sleep(delay);
                } catch (InterruptedException ex) {
                    System.err.println("Error in SQL Query execution ....");
                    System.err.println(ex);
                    System.exit(0);
                }
            }
        }
    }

    private static void UpdateOriginalTweet(RawTweet tweet) {
        try {
            String updateQuery = "Update public.\"Message\" SET \"IsNormalized\" = true WHERE \"Id\" = " + tweet.Id + ";";
            Statement st = db.createStatement();
            st.executeUpdate(updateQuery);

        } catch (SQLException ex) {
            System.err.println("Error in SQL Query execution ....");
            System.err.println(ex);
            System.exit(0);
        }
    }

    private static void SaveNormalizedTweet(RawTweet tweet) {
        try {
            String sql = "INSERT INTO public.\"NormalizedMsg\" (\"OriginalId\", \"MsgText\", \"Longitude\", "
                    + "\"Latitude\", \"CreatedTime\", \"PlaceCountry\", \"PlaceCountryCode\", \"PlaceFullName\", "
                    + "\"PlaceTwitterId\", \"PlaceName\", \"PlaceType\", \"PlacePolygon\", \"RetweetCount\", \"UserLocation\", \"IsProcessed\", \"NoTags\", \"IsScoreCalculated\")"
                    + "VALUES (?, ?, ?, ?,"
                    + "?, ?, ?, ?, ?,"
                    + "?, ?, ?, ?, ?, ?,?,?);";

            PreparedStatement preparedStatement = db.prepareStatement(sql);
            preparedStatement.setLong(1, tweet.Id);//raw record Id
            preparedStatement.setString(2, tweet.MsgText);
            preparedStatement.setFloat(3, tweet.Longitude);
            preparedStatement.setFloat(4, tweet.Latitude);            
            preparedStatement.setTimestamp(5, new java.sql.Timestamp(tweet.CreatedTime.getTime()));
            preparedStatement.setString(6, tweet.PlaceCountry);
            preparedStatement.setString(7, tweet.PlaceCountryCode);
            preparedStatement.setString(8, tweet.PlaceFullName);
            preparedStatement.setString(9, tweet.PlaceTwitterId);
            preparedStatement.setString(10, tweet.PlaceName);
            preparedStatement.setString(11, tweet.PlaceType);
            preparedStatement.setString(12, tweet.PlacePolygon);
            preparedStatement.setInt(13, tweet.RetweetCount);
            preparedStatement.setString(14, tweet.UserLocation);
            preparedStatement.setBoolean(15, false);
            preparedStatement.setBoolean(16, false);
            preparedStatement.setBoolean(17, false);

            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            System.err.println("Error in SQL Query execution ....");
            System.err.println(ex);
            System.exit(0);
        }

    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        main(null);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping ...");
    }

    @Override
    public void destroy() {
        System.out.println("Stopped !");
    }
}
