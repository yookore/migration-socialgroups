import org.junit.Assert._
import org.junit.Test
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.scalatest.matchers.MustMatchers
import org.scalatest.WordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitSuite
import org.apache.spark._
import scala.util.parsing.json._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

class AppTest extends JUnitSuite {

  var vcapApp: String = _

  @Before
  def setup() {
    vcapApp = """
      {
        "VCAP_APPLICATION": {
          "application_name": "uas",
          "application_uris": [
          "uas.apps.yookore.net"
          ],
          "application_version": "aca8f07c-ee8c-40af-b391-610c092f656a",
          "limits": {
          "disk": 1024,
          "fds": 16384,
          "mem": 2048
          },
          "name": "uas",
          "space_id": "e56ef682-e0cb-4b39-a3f5-96c461c414f8",
          "space_name": "dev",
          "uris": [
          "uas.apps.yookore.net"
          ],
          "users": null,
          "version": "aca8f07c-ee8c-40af-b391-610c092f656a"
        }
      }"""
  }

  @Test
  def testVcapSpaceName = {
    val vcapMap = JSON.parseFull(vcapApp)
    val m = vcapMap.get.asInstanceOf[Map[String, String]]
    val rootNode = m.get("VCAP_APPLICATION").get.asInstanceOf[Map[String, String]]
    val spaceName = rootNode.get("space_name")
    println("cf space_name: " + spaceName.get)
    assertNotNull(vcapMap)
  }

  @Test
  def testSerializeCaseClass = {
    implicit val formats = Serialization.formats(NoTypeHints)
          
                  
      val ser = write(Profile("An avid learner", "165774c2-a0e7-4a24-8f79-0f52bf3e2cda", "2015-07-13T07:48:47.924Z"))
      println(ser)
      assertNotNull(read[Profile](ser))
  }
}

case class Profile(biography: String,
        userid: String,
        creationdate: String) extends java.io.Serializable
