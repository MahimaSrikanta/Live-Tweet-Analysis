package itu.bigdata;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.URLEntity;
import itu.bigdata.tools.SentimentAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.Random;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * A spout that uses Twitter streaming API for continuously
 * getting tweets
 */
public class TweetSpout extends BaseRichSpout
{
  // Twitter API authentication credentials
  String custkey, custsecret;
  String accesstoken, accesssecret;
  String keyword;

  // To output tuples from spout to the next stage bolt
  SpoutOutputCollector collector;

  // Twitter4j - twitter stream to get tweets
  TwitterStream twitterStream;

  // Shared queue for getting buffering tweets received
  LinkedBlockingQueue<String> queue = null;
  
  Pattern moodPattern = Pattern.compile("love|hate|happy|angry|sad");
  Pattern properPattern = Pattern.compile("^[a-zA-Z0-9 ]+$");
  
  // Class for listening on the tweet stream - for twitter4j
  private class TweetListener implements StatusListener {

    // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status)
    {
      // add the tweet into the queue buffer
      String geoInfo = "";
      String urlInfo = "n/a";
      String status_text = String.valueOf(status.getText());
      String[] geo_array = new String[]{"40.7143528,-74.00597309999999", "40.7834345,-73.9662495", "34.0522342,-118.2436849", "41.8781136,-87.6297982", "29.7601927,-95.36938959999999", "39.952335,-75.16378900000001", "33.4483771,-112.0740373", "29.4241219,-98.49362819999999", "32.7153292,-117.1572551", "32.802955,-96.769923", "37.3393857,-121.8949555", "30.3321838,-81.65565099999999", "39.7685155,-86.1580736", "37.7749295,-122.4194155", "30.267153,-97.7430608", "39.9611755,-82.99879419999999", "32.725409,-97.3208496", "35.2270869,-80.8431267", "42.331427,-83.0457538", "42.3288623,-83.1206665", "31.7587198,-106.4869314", "31.8039824,-106.2051484", "35.1495343,-90.0489801", "39.2903848,-76.6121893", "42.3584308,-71.0597732", "47.6062095,-122.3320708", "38.8951118,-77.0363658", "36.1658899,-86.7844432", "39.7391536,-104.9847034", "39.740324,-104.9596773", "38.2526647,-85.7584557", "43.0389025,-87.9064736", "45.5234515,-122.6762071", "36.114646,-115.172816", "36.1398498,-115.188916", "35.4675602,-97.5164276", "35.5006256,-97.6114217", "35.0844909,-106.6511367", "32.2217429,-110.926479", "36.7477272,-119.7723661", "36.9858984,-119.2320784", "38.5815719,-121.4943996", "33.8041667,-118.1580556", "39.0997265,-94.5785667", "33.4151843,-111.8314724", "36.8529263,-75.97798499999999", "33.7489954,-84.3879824", "38.8338816,-104.8213634", "41.2523634,-95.99798829999999", "35.772096,-78.6386145", "25.7889689,-80.2264393", "41.4994954,-81.6954088", "36.1539816,-95.99277500000001", "36.2740199,-96.2375947", "37.8043637,-122.2711137", "44.9799654,-93.26383609999999", "37.6922222,-97.3372222", "32.735687,-97.10806559999999", "35.3732921,-119.0187125", "29.95106579999999,-90.0715323", "21.3069444,-157.8583333", "21.451996,-158.0954459", "33.8352932,-117.9145036", "27.950575,-82.4571776", "39.7294319,-104.8319195", "33.7455731,-117.8678338", "38.6270025,-90.19940419999999", "38.6105426,-90.3371889", "40.44062479999999,-79.9958864", "27.8005828,-97.39638099999999", "33.9533487,-117.3961564", "39.1031182,-84.5120196", "38.0405837,-84.5037164", "61.2180556,-149.9002778", "37.9577016,-121.2907796", "41.6639383,-83.55521200000001", "44.95416669999999,-93.11388889999999", "40.735657,-74.1723667", "36.0726354,-79.7919754", "42.88644679999999,-78.8783689", "33.0198431,-96.6988856", "40.806862,-96.681679", "36.0395247,-114.9817213", "41.079273,-85.1393513", "40.72815749999999,-74.0776417", "27.7730556,-82.64", "32.6400541,-117.0841955", "36.8507689,-76.28587259999999", "28.5383355,-81.3792365", "33.3061605,-111.8412502", "27.506407,-99.5075421", "43.0730517,-89.4012302", "36.09985959999999,-80.244216", "33.5778631,-101.8551665", "30.4582829,-91.1403196", "35.9940329,-78.898619", "32.912624,-96.63888329999999", "33.5386523,-112.1859866", "39.5296329,-119.8138027", "25.8575963,-80.2781057", "36.7682088,-76.2874927", "33.4941704,-111.9260519", "36.1988592,-115.1175013", "32.8140177,-96.9488945", "37.5482697,-121.9885719", "33.6839473,-117.7946942", "33.5206608,-86.80248999999999", "43.16103,-77.6109219", "42.938004,-77.3783789", "41.8036111,-74.2633333", "34.1083449,-117.2897652", "47.6587802,-117.4260466", "47.5909399,-117.2776573", "33.3528264,-111.789027", "38.8799697,-77.1067698", "37.219841,-76.003956", "38.8816208,-77.09098089999999", "32.3668052,-86.2999689", "43.612631,-116.211076", "37.5407246,-77.4360481", "41.6005448,-93.6091064", "37.63909719999999,-120.9968782", "35.0526641,-78.87835849999999", "32.5251516,-93.7501789", "41.0814447,-81.51900529999999", "47.2528768,-122.4442906", "41.7605849,-88.32007150000001", "34.1975048,-119.1770516", "34.0922335,-117.435048", "40.9312099,-73.89874689999999", "33.474246,-82.00967", "30.6943566,-88.04305409999999", "30.7921394,-88.24611829999999", "34.7464809,-92.28959479999999", "33.9424658,-117.2296717", "34.1425078,-118.255075", "35.2219971,-101.8312969", "33.660297,-117.9992265", "32.4609764,-84.9877094", "42.9633599,-85.6680863", "43.0020023,-85.57150150000001", "40.7607793,-111.8910474", "40.507092,-113.0011989", "30.4382559,-84.28073289999999", "42.2625932,-71.8022934", "42.2571058,-71.8571331", "37.0870821,-76.4730122", "34.7303688,-86.5861037", "35.9606384,-83.9207392", "41.8239891,-71.4128343", "34.3916641,-118.542586", "32.7459645,-96.99778459999999", "25.9017472,-97.4974838", "32.2987573,-90.1848103", "38.9822282,-94.6707917", "33.7739053,-117.9414477", "38.4404674,-122.7144314", "35.0456297,-85.3096801", "33.1958696,-117.3794834", "26.1223084,-80.14337859999999", "34.10639889999999,-117.5931084", "27.2758333,-80.35499999999999", "34.0633443,-117.6508876", "45.6387281,-122.6614861", "33.4255104,-111.9400054", "37.2089572,-93.29229889999999", "34.6867846,-118.1541632", "44.0520691,-123.0867536", "26.0122378,-80.3152233", "44.9428975,-123.0350963", "26.5628537,-81.9495331", "33.5805955,-112.2373779", "43.5499749,-96.700327", "42.1014831,-72.589811", "38.4087993,-121.3716178", "42.2711311,-89.0939952", "34.5794343,-118.1164613", "33.8752935,-117.5664384", "36.6777372,-121.6555013", "34.0552267,-117.7523048", "29.6910625,-95.2091006", "41.525031,-88.0817251", "40.9167654,-74.17181099999999", "39.114053,-94.6274636", "39.5528129,-87.9394779", "39.0653602,-94.5624426", "33.8358492,-118.3406288", "43.0481221,-76.14742439999999", "43.114397,-76.2710833", "41.1865478,-73.19517669999999", "37.6688205,-122.0807964", "40.5852602,-105.084423", "33.1192068,-117.086421", "39.7047095,-105.0813734", "41.7858629,-88.1472893", "39.7589478,-84.1916069", "26.0112014,-80.1494901", "37.36883,-122.0363496", "38.8048355,-77.0469214", "32.76679550000001,-96.5991593", "37.0298687,-76.34522179999999", "34.1477849,-118.1445155", "33.7877944,-117.8531119", "32.0835407,-81.09983419999999", "35.79154,-78.7811169", "33.8702923,-117.925338", "42.49299999999999,-83.02819699999999", "36.5297706,-87.3594528", "33.1972465,-96.6397822", "26.2034071,-98.23001239999999", "41.3081527,-72.9281577", "41.3583849,-72.7570024", "42.5803122,-83.0302033", "40.6916132,-112.0010501", "34.0007104,-81.0348144", "31.1171194,-97.72779589999999", "39.0558235,-95.68901849999999", "34.1705609,-118.8375937", "41.9778795,-91.6656232", "38.8813958,-94.81912849999999", "40.6639916,-74.2107006", "31.549333,-97.1466695", "41.76371109999999,-72.6850932", "36.3302284,-119.2920585", "29.6516344,-82.32482619999999", "34.2694474,-118.781482", "41.0534302,-73.5387341", "47.610377,-122.2006786", "38.8274243,-77.0089452", "37.9779776,-122.0310733", "25.9756704,-80.28675009999999", "26.271192,-80.2706044", "30.2240897,-92.0198427", "32.7765656,-79.93092159999999", "32.9756415,-96.8899636", "38.7521235,-121.2880059", "39.8680412,-104.9719243", "30.080174,-94.1265562", "40.6084305,-75.4901833", "33.639099,-112.3957576", "37.9715592,-87.5710898", "32.4487364,-99.73314390000002", "33.1506744,-96.82361159999999", "39.0911161,-94.41550679999999", "37.3541079,-121.9552356", "39.78172130000001,-89.6501481", "38.1040864,-122.2566367", "34.5361067,-117.2911565", "33.955802,-83.3823656", "40.6936488,-89.5889864", "42.732535,-84.5555347", "42.7563594,-84.5283267", "42.2808256,-83.7430378", "42.3076493,-83.8473015", "34.0686206,-118.0275667", "33.2148412,-97.13306829999999", "37.8715926,-122.272747", "40.2338438,-111.6585337", "33.94001430000001,-118.1325688", "31.9973456,-102.0779146", "35.2225668,-97.4394777", "41.5581525,-73.0514965", "33.6411316,-117.9186689", "33.9616801,-118.3531311", "42.9956397,-71.4547891", "35.8456213,-86.39027", "38.9517053,-92.3340724", "42.0372487,-88.2811895", "27.9658533,-82.8001026", "25.9420377,-80.2456045", "44.0216306,-92.4698992", "38.2544472,-104.6091409", "42.6334247,-71.31617179999999", "34.2257255,-77.9447102", "34.2746405,-119.2290053", "39.8027644,-105.0874842", "39.8366528,-105.0372046", "34.0686208,-117.9389526", "45.5001357,-122.4302013", "33.9022367,-118.081733", "46.8771863,-96.7898034", "33.1580933,-117.3505939", "38.24935809999999,-122.0399663", "42.3726399,-71.10965279999999", "33.9137085,-98.4933873", "35.9556923,-80.0053176", "45.7832856,-108.5006904", "44.51915899999999,-88.019826", "40.6096698,-111.9391031", "37.9357576,-122.3477486", "33.5539143,-117.2139232", "34.1808392,-118.3089661", "37.3205556,-121.9316667", "28.0344621,-80.5886646", "47.9789848,-122.2020794", "43.0125274,-83.6874562", "43.0777289,-83.67739279999999", "38.0049214,-121.805789", "42.12922409999999,-80.085059", "42.0366785,-80.0087746", "41.6833813,-86.25000659999999", "37.6879241,-122.4702079", "39.5807452,-104.8771726", "33.4936391,-117.1483648"};
      int rnd = new Random().nextInt(geo_array.length);
      if(status.getGeoLocation() == null)
      {
        geoInfo = geo_array[rnd];
      }
      else
      {
        geoInfo = String.valueOf(status.getGeoLocation().getLatitude()) + "," + String.valueOf(status.getGeoLocation().getLongitude());
      }
      if(status_text.toLowerCase().contains(keyword))
      {
          if(status.getURLEntities().length > 0)
          {
            for(URLEntity urlE: status.getURLEntities())
            {
              urlInfo = urlE.getURL();
            }         
          }
           queue.offer(status.getText() + "DELIMITER" + geoInfo + "DELIMITER" + urlInfo);
      }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn)
    {
    }

    @Override
    public void onTrackLimitationNotice(int i)
    {
    }

    @Override
    public void onScrubGeo(long l, long l1)
    {
    }

    @Override
    public void onStallWarning(StallWarning warning)
    {
    }

    @Override
    public void onException(Exception e)
    {
      e.printStackTrace();
    }
  };

  /**
   * Constructor for tweet spout that accepts the credentials
   */
    public TweetSpout(
      String                key,
      String                secret,
      String                token,
      String                tokensecret,
      String strkey)
  {
    custkey = key;
    custsecret = secret;
    accesstoken = token;
    accesssecret = tokensecret;
    keyword = strkey;
  }


  @Override
  public void open(
      Map                     map,
      TopologyContext         topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
    // create the buffer to block tweets
    queue = new LinkedBlockingQueue<String>(1000);
    SentimentAnalyzer.init();
    // save the output collector for emitting tuples
    collector = spoutOutputCollector;


    // build the config with credentials for twitter 4j
    ConfigurationBuilder config =
        new ConfigurationBuilder()
               .setOAuthConsumerKey(custkey)
               .setOAuthConsumerSecret(custsecret)
               .setOAuthAccessToken(accesstoken)
               .setOAuthAccessTokenSecret(accesssecret);

    // create the twitter stream factory with the config
    TwitterStreamFactory fact =
        new TwitterStreamFactory(config.build());

    // get an instance of twitter stream
    twitterStream = fact.getInstance();    
    
    FilterQuery tweetFilterQuery = new FilterQuery();
    double[][] location = {{-122.75,31.8,-101.75,37.8},{-74,40,-73,41}};
    tweetFilterQuery.language(new String[]{"en"});
    tweetFilterQuery.track(new String[]{keyword});
    
 
    
    // provide the handler for twitter stream
    twitterStream.addListener(new TweetListener());
    tweetFilterQuery.track(new String[]{keyword});
    twitterStream.filter(tweetFilterQuery);
    tweetFilterQuery.locations(location).track(new String[]{keyword});
    //tweetFilterQuery.track(new String[]{keyword});

    // start the sampling of tweets
    //twitterStream.sample();
  
  }

  @Override
  public void nextTuple()
  {
    // try to pick a tweet from the buffer
    String ret = queue.poll();
    String geoInfo;
    String originalTweet;
    try {
    String file = "tweets.log";
    FileWriter fileWriter = new FileWriter(file,true);
    // if no tweet is available, wait for 50 ms and return
    if (ret==null)
    {
      Utils.sleep(50);
      return;
    }
    else
    {
        fileWriter.write("\nret is =>");
        System.out.println(ret);
        fileWriter.write(ret);
        fileWriter.flush();
        fileWriter.close();
        geoInfo = ret.split("DELIMITER")[1];
        originalTweet = ret.split("DELIMITER")[0];
    }

    
    if(geoInfo != null && !geoInfo.equals("n/a"))
    {
        System.out.print("\t DEBUG SPOUT: BEFORE SENTIMENT \n");
        int sentiment = SentimentAnalyzer.findSentiment(originalTweet)-2;
        System.out.print("\t DEBUG SPOUT: AFTER SENTIMENT (" + String.valueOf(sentiment) + ") for \t" + originalTweet + "\n");
        collector.emit(new Values(ret, sentiment));
    }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close()
  {
    // shutdown the stream - when we are going to exit
    twitterStream.shutdown();
  }

  /**
   * Component specific configuration
   */
  @Override
  public Map<String, Object> getComponentConfiguration()
  {
    // create the component config
    Config ret = new Config();

    // set the parallelism for this spout to be 1
    ret.setMaxTaskParallelism(1);

    return ret;
  }

  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet'
    outputFieldsDeclarer.declare(new Fields("tweet", "sentiment"));
  }
}
