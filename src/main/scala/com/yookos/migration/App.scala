package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object App extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  createSchema(conf)
  
  implicit val formats = DefaultFormats

  // serializes objects from redis into
  // desired types
  //import com.redis.serialization._
  //import Parse.Implicits._
  
  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_group_index") == null) 0 else cache.get("latest_legacy_group_index").toInt

  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    //"dbtable" -> "legacyusers")
    "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers")
  )

  
  val groupsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jivesgroup")
  )
  
  val df = mappingsDF.select(mappingsDF("userid"), mappingsDF("yookoreid"), mappingsDF("username"))

  reduceGroups(df)
  
  /*if (cachedIndex != 0) 
    implicit val index = cachedIndex reduce(df, cachedIndex) 
  else reduce(df)*/   
  
  /**
   * Resume migration based on index
   */ 
  protected def reduceByIndex(df: DataFrame, index: Long) = {
    val offset = s"offset $index" 
    df.filter(offset).collect().foreach(row => {
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }

  def reduceGroups(df: DataFrame) = {
    df.collect().foreach(row => {
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_group_index", cachedIndex.toString)
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }

  def upsert(row: Row, userid: Long) = {
    groupsDF.select(
      groupsDF("groupid"), groupsDF("grouptype"), groupsDF("userid"), 
      groupsDF("name"), groupsDF("displayname"),
      groupsDF("description"), groupsDF("status"),
      groupsDF("creationdate"), groupsDF("modificationdate"))
        .filter(f"userid = $userid%d").foreach {
          // cache latest row value and latest index
          // cache.set("latest_legacy_group_item", "")
          groupItem => 
            val id = java.util.UUID.randomUUID().toString()
            val groupid = groupItem.getLong(0)
            val grouptype = groupItem.getInt(1)
            val jiveuserid = userid // needed for legacy comments on status
            val yookoreid = row.getString(1)
            val author = row.getString(2) // same as username
            val name = groupItem.getString(3)
            val displayname = groupItem.getString(4)
            val desc = groupItem.getString(5)
            val status = groupItem.getInt(6)
            val creationdate = groupItem.getLong(7)
            val modificationdate = groupItem.getLong(8)

            sc.parallelize(Seq(SocialGroup(
              id, groupid, grouptype, yookoreid, author, jiveuserid, 
              name, displayname, desc, status, 
              creationdate, modificationdate)))
                .saveToCassandra(s"$keyspace", "legacysocialgroups", 
                  SomeColumns("id", "groupid", "grouptype", "userid",
                    "author", "jiveuserid", "name", "displayname",
                    "description", "status",
                    "creationdate", "modificationdate"
                  )
                )
        }
        println("===Latest group cachedIndex=== " + cache.get("latest_legacy_group_index").toInt)
    
  }
  
  groupsDF.printSchema()

  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
  println("===keyspace=== " + keyspace)
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacysocialgroups ( " +
        "id text, " + 
        "groupid bigint, " +
        "userid text, " + 
        "author text, " + 
        "jiveuserid bigint, " + 
        "grouptype text, " + 
        "name text, " + 
        "displayname text, " + 
        "creationdate bigint, " + 
        "modificationdate bigint, " + 
        "description text, " + 
        "status text, " + 
        "PRIMARY KEY ((id, userid), author) ) " + 
      "WITH CLUSTERING ORDER BY (author DESC)")
    } wasApplied
  }

  /*def reduce(df: DataFrame)(implicit index: Long) = index match {
    case None => reduceGroups(df)
    case _:Int => reduceGroups(df, index)
  }*/
}
