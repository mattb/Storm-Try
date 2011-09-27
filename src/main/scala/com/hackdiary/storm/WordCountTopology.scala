package com.hackdiary.storm

import backtype.storm._
import backtype.storm.task._
import backtype.storm.topology._
import backtype.storm.spout._
import backtype.storm.tuple._
import collection.JavaConversions._
import backtype.storm.utils.Utils.tuple
import backtype.storm.utils.Utils
import twitter4j._
import twitter4j.conf._
import redis.clients.jedis.Jedis

object WordCountTopology {
  def main(args:Array[String]) = {
    println(System.getProperties.getProperty("java.class.path"))
    val builder = new TopologyBuilder

    builder.setSpout(1, new WordSpout)
    builder.setBolt(2, new WordCount, 8).fieldsGrouping(1, new Fields("words"))
    builder.setBolt(3, new JsonOut).shuffleGrouping(2)

    val conf = new java.util.HashMap[Any,Any]
    conf.put(Config.TOPOLOGY_DEBUG, true)

    val cluster = new LocalCluster

    cluster.submitTopology("test", conf, builder.createTopology)
  }
}

class WordSpout extends IRichSpout {
  var collector : SpoutOutputCollector = _
  var stream : StatusStream = _
  var listener : StatusListener = _

  override def ack(msgId : Object) = {
  }

  override def close = {
  }

  override def fail(msgId : Object) = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields("words"))
  }
  override def isDistributed : Boolean = {
    false
  }
  override def open(conf : java.util.Map[_,_], context : TopologyContext, collector : SpoutOutputCollector) {
    this.stream = new TwitterStreamFactory(TwitterConfig.config.build).getInstance.getSampleStream
    // this.stream = new TwitterStreamFactory(TwitterConfig.config.build).getInstance.getFilterStream(new FilterQuery().track(Array("london")))

    this.collector = collector
    this.listener = new StatusAdapter { 
      override def onStatus(status : Status) = {
        val tokenizer = new org.apache.lucene.analysis.standard.StandardTokenizer(org.apache.lucene.util.Version.LUCENE_34,new
          java.io.StringReader(status.getText))
        val shingles = new org.apache.lucene.analysis.shingle.ShingleFilter(tokenizer)
        while(shingles.incrementToken) {
          val token = shingles.getAttribute(classOf[org.apache.lucene.analysis.tokenattributes.CharTermAttribute])
          if(token.toString.contains(" ")) collector.emit(tuple(token.toString.toLowerCase))
        }
      }
    }
  }
  override def nextTuple = {
    stream.next(listener)
  }
}

class WordCount extends IBasicBolt {
  var counts : Map[String,Int] = Map.empty

  override def prepare(conf : java.util.Map[_,_], context : TopologyContext) = {
  }

  override def execute(tuple : Tuple, collector : BasicOutputCollector) = {
    val word : String = tuple.getString(0)
    var count : Int = counts.getOrElse(word, 0) + 1
    counts += (word -> count)
    if(count > 10) collector.emit(new Values(word, count : java.lang.Integer))
  }

  override def cleanup() = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields("word", "count"))
  }
}

class JsonOut extends IBasicBolt {
  var jedis : Jedis = _
  override def prepare(conf : java.util.Map[_,_], context : TopologyContext) = {
    jedis = new Jedis("localhost")
  }

  override def execute(tuple : Tuple, collector : BasicOutputCollector) = {
    //val data = for((field, i) <- tuple.getFields.zipWithIndex) yield (field -> tuple.getValue(i))
    //println(data toMap)
    jedis.set(tuple.getString(0), tuple.getInteger(1).toString)
    jedis.zadd("top", tuple.getInteger(1).toDouble, tuple.getString(0))
  }

  override def cleanup() = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
  }
}
