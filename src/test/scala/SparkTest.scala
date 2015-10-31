import org.junit.Assert._
import org.junit.Test
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import akka.actor.{ Actor, Props, ActorSystem }
import akka.testkit.{ TestActors, TestKit, TestActorRef, ImplicitSender }
import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitSuite
import org.apache.spark._
import scala.util.parsing.json._

import com.datastax.spark.connector.mapper._

import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

case class Address(city: String, name: String, lastupdated: String, creationdate: String, state: String, terms: Boolean) extends java.io.Serializable

object Address extends App { 
    implicit object Mapper extends DefaultColumnMapper[Address](
      Map("name" -> "name", "city" -> "city", "state" -> "state", "terms" -> "address", "lastupdated" -> "lastupdated")) 
  
  val cs = this.getClass.getConstructors
  def createFromList(params: Seq[Any]) = params match {
    case Seq(city:Any, 
              name:Any, 
              lastupdated:Any, 
              creationdate: Any,
              state:Any, 
              terms: Boolean) => Address(StringEscapeUtils.escapeJava(city.asInstanceOf[String]),
                                  StringEscapeUtils.escapeJava(name.asInstanceOf[String]),
                                  StringEscapeUtils.escapeJava(lastupdated.asInstanceOf[String]),
                                  StringEscapeUtils.escapeJava(creationdate.asInstanceOf[String]),
                                  StringEscapeUtils.escapeJava(state.asInstanceOf[String]), 
                                  terms.booleanValue())
  }

  //implicit def orderingByName[A <: Address]: Ordering[A] =
  //      Ordering.by(a => (a.name, a.city))

  //def apply[T](addresses: Seq[Address[T]])(implicit ordering: Ordering[T]) = {}
}

class SparkTest extends JUnitSuite {
    
  var sc: SparkContext = _

  @Before
  def setup() {
    println("Setting up")
    val driverPort = 7077
    val driverHost = "localhost"
    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("Yookore Social Groups") // name in Spark web UI
      .set("spark.driver.port", driverPort.toString)
      .set("spark.driver.host", driverHost)
      .set("spark.logConf", "true")
      .set("spark.akka.logLifecycleEvents", "true")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", "localhost")
    
    sc = new SparkContext(conf)
  }

  @Test
  def testMap = {
    val rdd = sc.parallelize(
        """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    //val transformer = rdd.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
    val transformer = rdd.flatMap(_.split("\\s+"))
    transformer.collect().foreach(println)
  }

  @Test
  def testTransformPlainText = {
    sc.textFile("/tmp/people.txt")
      .map { line =>
        val array = line.split(",", 2)
        (array(0), array(1))  
      }.flatMap {
        case (id, contents) => toWords(contents).map(w => ((w, id), 1))
      }.reduceByKey {
        (count1, count2) => count1 + count2
      }.map {
        case ((word, path), n) => (word, (path, n))
      }.groupByKey
      .map {
        case (word, list) => (word, sortByCount(list))
      }.saveAsTextFile("/tmp/people-processed.txt")
  }

  def typeOf[T](o: Seq[T]) = o match {
    case _: Seq[_] => "Address"
    case _         => "Unknown"
  }

  def typeOfM[T: Manifest](t: T): Manifest[T] = manifest[T]

  @Test
  def testStringInterpolation = {
    val v:Any = "2015-07-13T07:48:47.924Z"
    val str = StringEscapeUtils.escapeJava(v.asInstanceOf[String])
    println("str after interpolation: " + str)
    assertNotNull(str)
  }

  @Test
  def testJsonMap = {
    val rdd = sc.parallelize("""{"name":"Yin","city":"Columbus","state":"Ohio","terms":true,"lastupdated":"2015-07-13T07:48:47.924Z","creationdate":"2015-07-13T07:48:47.925Z"}""" :: Nil)
    //val rdd = """{"name":"Yin","city":"Columbus","state":"Ohio","terms":true,"lastupdated":"2015-07-13T07:48:47.924Z"}""" :: Nil
    val jmap = rdd.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
   println(jmap)
   val values = jmap.map {
     //m => Seq() :+ Address.createFromList(m.toMap.values.toList)
     m => Seq() :+ Address.createFromList(m.values.to[collection.immutable.Seq])
   }
   //val values = jmap.map(m => Seq() :+ m.values.toList)
   //val values = Seq() :+ Address.createFromList(jmap.map(m => m.values.toList))
   //values.saveToCassandra("yookore", "location")
   //foreach type that's a string, toString it
   //values.foreach(i => if (typeOf(i) == "scala.collection.Seq[Address]") println (i.toString))
   values.foreach(i => println (i.sorted(Ordering.by((_: Address).name))))
    
  }

  def mapToSeq(m: Map[String, String]): scala.collection.immutable.Iterable[Seq[String]] = {
    //val m = Map(("a" -> "animal"))
    val s = Seq[String]()
    m.map { 
      case (k,v) => s :+ v
      //s
    } 
  }

  def mapToSeqRDD(rdd: Map[String, Any]): scala.collection.immutable.Iterable[Seq[Any]] = {
    val s = Seq[String]()
    rdd.map {
      case m: Map[_,_] => s :+ m.values
    }
  }

  def testSeq = {
    val totalUsers = 1
    val timeid = com.eaio.uuid.UUIDGen.newTime().toString
    case class Users(id:String, total: Long)
    val userSeq = Seq(Users(timeid, totalUsers))
    //val userSeq = Seq(timeid, totalUsers)
    var users = sc.parallelize(userSeq)
    println(users)
    users.foreach(println)
  }

  @After
  def tearDown() {
    println("@After - tearDown");
    sc.stop()
  }
}

