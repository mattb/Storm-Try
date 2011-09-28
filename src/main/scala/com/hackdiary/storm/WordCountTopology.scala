package com.hackdiary.storm

import backtype.storm._
import backtype.storm.task._
import backtype.storm.topology._
import backtype.storm.spout._
import backtype.storm.tuple._
import collection.JavaConversions._
import backtype.storm.utils.Utils
import twitter4j._
import twitter4j.conf._
import redis.clients.jedis.Jedis
import com.cybozu.labs.langdetect._

object WordCountTopology {
  def main(args:Array[String]) = {
    DetectorFactory.loadProfile("profiles")

    val builder = new TopologyBuilder

    builder.setSpout(1, new WordSpout)
    builder.setBolt(2, new LanguageDetect, 8).shuffleGrouping(1)
    builder.setBolt(3, new SplitTerms, 2).shuffleGrouping(2)
    builder.setBolt(4, new WordCount, 8).fieldsGrouping(3, new Fields("words"))
    builder.setBolt(5, new RedisOut).shuffleGrouping(4)

    val conf = Map(Config.TOPOLOGY_DEBUG -> false)

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
        collector.emit(Utils.tuple(status.getText))
      }
    }
  }
  override def nextTuple = {
    stream.next(listener)
  }
}

class SplitTerms extends IRichBolt {
  var collector : OutputCollector = _

  override def prepare(conf : java.util.Map[_,_], context : TopologyContext, collector : OutputCollector) = {
    this.collector = collector
  }

  override def execute(tuple : Tuple) = {
    val tokenizer = new org.apache.lucene.analysis.standard.StandardTokenizer(org.apache.lucene.util.Version.LUCENE_34,new
      java.io.StringReader(tuple.getString(0)))
    val shingles = new org.apache.lucene.analysis.shingle.ShingleFilter(tokenizer,3)
    while(shingles.incrementToken) {
      val token = shingles.getAttribute(classOf[org.apache.lucene.analysis.tokenattributes.CharTermAttribute])
      if(token.toString.contains(" ")) collector.emit(Utils.tuple(token.toString.toLowerCase, tuple.getString(1)))
    }
    collector.ack(tuple)
  }

  override def cleanup() = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields("words","lang"))
  }
}

class LanguageDetect extends IRichBolt {
  var collector : OutputCollector = _

  override def prepare(conf : java.util.Map[_,_], context : TopologyContext, collector : OutputCollector) = {
    this.collector = collector
  }

  override def execute(tuple : Tuple) = {
    val detector = DetectorFactory.create
    try {
      detector.append(tuple.getString(0))
      collector.emit(Utils.tuple(tuple.getString(0),detector.detect))
      collector.ack(tuple)
    } catch {
      case e : Exception => {
        collector.emit(Utils.tuple(tuple.getString(0),""))
        collector.ack(tuple)
      }
    }
  }

  override def cleanup() = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields("words", "lang"))
  }
}

class RedisOut extends IRichBolt {
  var jedis : Jedis = _
  var collector : OutputCollector = _

  override def prepare(conf : java.util.Map[_,_], context : TopologyContext, collector : OutputCollector) = {
    this.collector = collector
    this.jedis = new Jedis("localhost")
  }

  override def execute(tuple : Tuple) = {
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

class WordCount extends IRichBolt {
  var counts : Map[String,Int] = Map.empty
  var collector : OutputCollector = _

  override def prepare(conf : java.util.Map[_,_], context : TopologyContext, collector : OutputCollector) = {
    this.collector = collector
  }

  override def execute(tuple : Tuple) = {
    val word : String = tuple.getString(1) + ":" + tuple.getString(0)
    var count : Int = counts.getOrElse(word, 0) + 1
    counts += (word -> count)
    if(count > 10) collector.emit(new Values(word, count : java.lang.Integer))
    collector.ack(tuple)
  }

  override def cleanup() = {
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer) {
    declarer.declare(new Fields("word", "count"))
  }
}

