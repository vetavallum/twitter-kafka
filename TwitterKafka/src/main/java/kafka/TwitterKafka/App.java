package kafka.TwitterKafka;

import kafka.TwitterKafka.TweetListener;
import twitter4j.*;

public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        // Adding Listener to consume tweets
        StatusListener listener = (StatusListener) new TweetListener();
        twitterStream.addListener(listener);

        // filtering tweets that has content "android" or "iphone"
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track("android","ios","windows", "blackberry");

        // applying the filter
        twitterStream.filter(filterQuery);
    }
}
