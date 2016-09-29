package itu.bigdata;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

class ElectionSentimentsTopology
{
  public static void main(String[] args) throws Exception
  {
    //Variable TOP_N number of words
    int TOP_N = 5;
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout-1", new TweetSpout("KrixvGUVxCZLG8Jf6hUK9OPBe","FE7DSclPywqcoPqGMUHmS8iSp2zriyEpULpTqFlQ5iXnW6EVbz", "69214321-4vt0VU5L06qsgLiCVQ6R4LGie438D1pMeRzXPY9x4","BThYeAXlyMtwg9XymJ9F9Z4kwbpst0R0vmEpVDyLAR23L","trump"), 1);
    builder.setSpout("tweet-spout-2", new TweetSpout("KrixvGUVxCZLG8Jf6hUK9OPBe","FE7DSclPywqcoPqGMUHmS8iSp2zriyEpULpTqFlQ5iXnW6EVbz", "69214321-4vt0VU5L06qsgLiCVQ6R4LGie438D1pMeRzXPY9x4","BThYeAXlyMtwg9XymJ9F9Z4kwbpst0R0vmEpVDyLAR23L","hillary"), 1);
    builder.setSpout("tweet-spout-3", new TweetSpout("PSFtUt5b3uxrhTjJgY2PwnWnc","Rz5pKGmaGDAMKwwP5uFes7c27sirdAj5PZn58KJOY7GYrS2Tul","69214321-mTK4GrQ0WYDCbFtlAzfvhjFEnLTzkVEsWqoSNgH8Q","SQVMBWcbtJZVBCvEIazacoFocIDfF6LoEqsg8PK2WhHLh","clinton"), 1);
    builder.setSpout("tweet-spout-4", new TweetSpout("PSFtUt5b3uxrhTjJgY2PwnWnc","Rz5pKGmaGDAMKwwP5uFes7c27sirdAj5PZn58KJOY7GYrS2Tul","69214321-mTK4GrQ0WYDCbFtlAzfvhjFEnLTzkVEsWqoSNgH8Q","SQVMBWcbtJZVBCvEIazacoFocIDfF6LoEqsg8PK2WhHLh","donald"), 1);


    // attach the parse tweet bolt using shuffle grouping
    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout-1").shuffleGrouping("tweet-spout-2").shuffleGrouping("tweet-spout-3").shuffleGrouping("tweet-spout-4");
    builder.setBolt("infoBolt", new InfoBolt(), 10).fieldsGrouping("parse-tweet-bolt", new Fields("county_id"));
    builder.setBolt("top-words", new TopWords(), 10).fieldsGrouping("infoBolt", new Fields("county_id"));
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("top-words");

    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(4);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
