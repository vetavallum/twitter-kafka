package kafka.TwitterKafka;


import kafka.TwitterKafka.KafkaTwitter;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;


public class TweetListener implements StatusListener
{

//    @Override
    public void onStatus(Status status)
    {
        String tweet = status.getText().toLowerCase();
        String userId = status.getUser().getScreenName();
        if(tweet.contains("android") || tweet.contains("ios") || tweet.contains("blackberry") || tweet.contains("windows"))
        {
    //        System.out.println("Tweet>>> @" + userId + " - " + tweet);
            KafkaTwitter.send(userId, tweet);
        }
    }

//    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
    {

    }

//    @Override
    public void onTrackLimitationNotice(int i)
    {

    }

//    @Override
    public void onScrubGeo(long l, long l1)
    {

    }

//    @Override
    public void onStallWarning(StallWarning stallWarning)
    {

    }

//    @Override
    public void onException(Exception e)
    {

    }
}