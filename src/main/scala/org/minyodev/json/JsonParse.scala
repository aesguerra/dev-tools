package org.minyodev.json

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by aesguerra on 2/1/17.
  */
object JsonParse extends App {

  implicit val formats = DefaultFormats
  case class Hotspots(
                       id: Long,
                       date_entered: String,
                       company_id: String,
                       tenant_id: String,
                       wifiarea_id: String,
                       wrop_id: String,
                       identifier: String,
                       date_modified: String,
                       deleted: Int,
                       country: String,
                       city: String,
                       address: String,
                       building_id: Int,
                       floor_id: Int,
                       latitude: Float,
                       longitude: Float,
                       name: String,
                       description: String,
                       vendor: String,
                       firmware: String,
                       macaddress: String,
                       enabled: Int,
                       state: String,
                       zip: String,
                       tag: String,
                       nas_type: String)
  case class SplashPortal(
                           id: Long,
                           os: String,
                           date_entered: String,
                           company_id: Long,
                           tenant_id: Long,
                           wifiarea_id: String,
                           hotspot_id: Long,
                           browser: String,
                           agent: String,
                           browser_version: String,
                           ipaddress: String,
                           screen: String,
                           screen_width: String,
                           screen_height: String,
                           controller: String,
                           action: String,
                           username: String,
                           realm: String,
                           checksum: String,
                           device: String,
                           lang: String,
                           checked: Int,
                           mac_client: String,
                           browse_start_time: String,
                           browse_end_time: String,
                           pageload_time: String,
                           user_session: String,
                           portal_language: String,
                           app_name: String)
  case class WropAuthPost(
                           id: Int,
                           username: String,
                           company_id: String,
                           called_station_id: String,
                           nas_ip_address: String,
                           reply_message: String,
                           entered_date: String,
                           framed_ip_address: String,
                           result: String,
                           calling_station_id: String,
                           network_name: String,
                           session_timeout: Int,
                           network_space_id: String)
  case class RadiusAccounts(ID: String,
                            CREATED_BY: String,
                            DATE_ENTERED: String,
                            DATE_MODIFIED: String,
                            DELETED: Boolean,
                            MODIFIED_BY: String,
                            COMPANY_ID: Long,
                            COMPANY_PATH: String,
                            ACCT_AUTHENTIC: String,
                            ACCT_INPUT_OCTETS: Long,
                            ACCT_INTERIM_INTERVAL_DEADLINE: String,
                            ACCT_OUTPUT_OCTETS: Long,
                            ACCT_SESSION_ID: String,
                            ACCT_SESSION_TIME: Int,
                            ACCT_START_DELAY: Int,
                            ACCT_START_TIME: String,
                            ACCT_STOP_DELAY: Int,
                            ACCT_STOP_TIME: String,
                            ACCT_TERMINATE_CAUSE: String,
                            ACCT_UNIQUE_ID: String,
                            CALLED_STATION_ID: String,
                            CALLING_STATION_ID: String,
                            CHARGE_ID: String,
                            CHARGED: Boolean,
                            CONNECT_INFO_START: String,
                            CONNECT_INFO_STOP: String,
                            CUSTOMER_ID: String,
                            FRAMED_IP_ADDRESS: String,
                            FRAMED_PROTOCOL: String,
                            INTERNAL_SERVICE_TYPE: String,
                            NAS_IP_ADDRESS: String,
                            NAS_PORT_ID: String,
                            NAS_PORT_TYPE: String,
                            NETWORK_SPACE_ID: String,
                            REALM: String,
                            SERVICE_PROFILE: String,
                            SERVICE_TYPE: String,
                            STATUS: String,
                            STRIPPED_USERNAME: String,
                            USERNAME: String,
                            NAS_ID: String,
                            NAS_NETWORK_SPACE_ID: String)

  def extractToAfter(s: String): JValue = {
    parse(s) \ "payload" \ "after"
  }

  val splashStr = """ {"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"string","optional":true,"field":"os"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"field":"date_entered"},{"type":"int64","optional":true,"field":"company_id"},{"type":"int64","optional":true,"field":"tenant_id"},{"type":"string","optional":true,"field":"wifiarea_id"},{"type":"int64","optional":true,"field":"hotspot_id"},{"type":"string","optional":true,"field":"browser"},{"type":"string","optional":true,"field":"agent"},{"type":"string","optional":true,"field":"browser_version"},{"type":"string","optional":true,"field":"ipaddress"},{"type":"string","optional":true,"field":"screen"},{"type":"string","optional":true,"field":"screen_width"},{"type":"string","optional":true,"field":"screen_height"},{"type":"string","optional":true,"field":"controller"},{"type":"string","optional":true,"field":"action"},{"type":"string","optional":true,"field":"username"},{"type":"string","optional":true,"field":"realm"},{"type":"string","optional":true,"field":"checksum"},{"type":"string","optional":true,"field":"device"},{"type":"string","optional":true,"field":"lang"},{"type":"int16","optional":true,"field":"checked"},{"type":"string","optional":true,"field":"mac_client"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"browse_start_time"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"browse_end_time"},{"type":"string","optional":true,"field":"pageload_time"},{"type":"string","optional":true,"field":"user_session"},{"type":"string","optional":true,"field":"portal_language"},{"type":"string","optional":true,"field":"app_name"}],"optional":true,"name":"dbserver.test.splashportal_visits_history.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"string","optional":true,"field":"os"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"field":"date_entered"},{"type":"int64","optional":true,"field":"company_id"},{"type":"int64","optional":true,"field":"tenant_id"},{"type":"string","optional":true,"field":"wifiarea_id"},{"type":"int64","optional":true,"field":"hotspot_id"},{"type":"string","optional":true,"field":"browser"},{"type":"string","optional":true,"field":"agent"},{"type":"string","optional":true,"field":"browser_version"},{"type":"string","optional":true,"field":"ipaddress"},{"type":"string","optional":true,"field":"screen"},{"type":"string","optional":true,"field":"screen_width"},{"type":"string","optional":true,"field":"screen_height"},{"type":"string","optional":true,"field":"controller"},{"type":"string","optional":true,"field":"action"},{"type":"string","optional":true,"field":"username"},{"type":"string","optional":true,"field":"realm"},{"type":"string","optional":true,"field":"checksum"},{"type":"string","optional":true,"field":"device"},{"type":"string","optional":true,"field":"lang"},{"type":"int16","optional":true,"field":"checked"},{"type":"string","optional":true,"field":"mac_client"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"browse_start_time"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"browse_end_time"},{"type":"string","optional":true,"field":"pageload_time"},{"type":"string","optional":true,"field":"user_session"},{"type":"string","optional":true,"field":"portal_language"},{"type":"string","optional":true,"field":"app_name"}],"optional":true,"name":"dbserver.test.splashportal_visits_history.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"server_id"},{"type":"int64","optional":false,"field":"ts_sec"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"boolean","optional":true,"field":"snapshot"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"}],"optional":false,"name":"dbserver.test.splashportal_visits_history.Envelope","version":1},"payload":{"before":null,"after":{"id":20170201116240,"os":"iPad","date_entered":"2017-02-01T03:11:53+08:00","company_id":300002,"tenant_id":300002,"wifiarea_id":"200002","hotspot_id":100002,"browser":"Safari","agent":"Mozilla/5.0 (iPad; CPU OS 9_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13C75 Safari/601.1","browser_version":"9","ipaddress":"151.0.208.146","screen":"768x1024","screen_width":"768","screen_height":"1024","controller":"logged","action":"index","username":"USERFOO","realm":"1154.com","checksum":"jtfb0ymyhk5ap027kobps83gcet48ycgi","device":"tablet","lang":"it-it","checked":1,"mac_client":"cc785fc2e9d9","browse_start_time":1485947513000,"browse_end_time":null,"pageload_time":"0.6","user_session":"l7i0v0vji813ak6vjs394hl867","portal_language":"eng","app_name":""},"source":{"name":"dbserver","server_id":10010,"ts_sec":1485918713,"gtid":"b2506b1d-94c1-11e6-9bb3-1c1b0d26e666:1355","file":"mysql-bin.000106","pos":10268,"row":0,"snapshot":null},"op":"c","ts_ms":1485918713964}} """
  val radiusStr = """ {"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"field":"ID"},{"type":"string","optional":true,"field":"CREATED_BY"},{"type":"int64","optional":false,"name":"io.debezium.time.Timestamp","version":1,"field":"DATE_ENTERED"},{"type":"int64","optional":false,"name":"io.debezium.time.Timestamp","version":1,"field":"DATE_MODIFIED"},{"type":"boolean","optional":false,"field":"DELETED"},{"type":"string","optional":true,"field":"MODIFIED_BY"},{"type":"int64","optional":false,"field":"COMPANY_ID"},{"type":"string","optional":false,"field":"COMPANY_PATH"},{"type":"string","optional":true,"field":"ACCT_AUTHENTIC"},{"type":"int64","optional":true,"field":"ACCT_INPUT_OCTETS"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_INTERIM_INTERVAL_DEADLINE"},{"type":"int64","optional":true,"field":"ACCT_OUTPUT_OCTETS"},{"type":"string","optional":false,"field":"ACCT_SESSION_ID"},{"type":"int32","optional":true,"field":"ACCT_SESSION_TIME"},{"type":"int32","optional":true,"field":"ACCT_START_DELAY"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_START_TIME"},{"type":"int32","optional":true,"field":"ACCT_STOP_DELAY"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_STOP_TIME"},{"type":"string","optional":true,"field":"ACCT_TERMINATE_CAUSE"},{"type":"string","optional":true,"field":"ACCT_UNIQUE_ID"},{"type":"string","optional":true,"field":"CALLED_STATION_ID"},{"type":"string","optional":true,"field":"CALLING_STATION_ID"},{"type":"string","optional":true,"field":"CHARGE_ID"},{"type":"boolean","optional":true,"field":"CHARGED"},{"type":"string","optional":true,"field":"CONNECT_INFO_START"},{"type":"string","optional":true,"field":"CONNECT_INFO_STOP"},{"type":"string","optional":true,"field":"CUSTOMER_ID"},{"type":"string","optional":true,"field":"FRAMED_IP_ADDRESS"},{"type":"string","optional":true,"field":"FRAMED_PROTOCOL"},{"type":"string","optional":true,"field":"INTERNAL_SERVICE_TYPE"},{"type":"string","optional":true,"field":"NAS_IP_ADDRESS"},{"type":"string","optional":true,"field":"NAS_PORT_ID"},{"type":"string","optional":true,"field":"NAS_PORT_TYPE"},{"type":"string","optional":true,"field":"NETWORK_SPACE_ID"},{"type":"string","optional":true,"field":"REALM"},{"type":"string","optional":true,"field":"SERVICE_PROFILE"},{"type":"string","optional":true,"field":"SERVICE_TYPE"},{"type":"string","optional":true,"field":"STATUS"},{"type":"string","optional":true,"field":"STRIPPED_USERNAME"},{"type":"string","optional":true,"field":"USERNAME"},{"type":"string","optional":true,"field":"NAS_ID"},{"type":"string","optional":true,"field":"NAS_NETWORK_SPACE_ID"}],"optional":true,"name":"dbserver.test.RADIUS_ACCOUNTINGS_HISTORY.Value","field":"before"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"ID"},{"type":"string","optional":true,"field":"CREATED_BY"},{"type":"int64","optional":false,"name":"io.debezium.time.Timestamp","version":1,"field":"DATE_ENTERED"},{"type":"int64","optional":false,"name":"io.debezium.time.Timestamp","version":1,"field":"DATE_MODIFIED"},{"type":"boolean","optional":false,"field":"DELETED"},{"type":"string","optional":true,"field":"MODIFIED_BY"},{"type":"int64","optional":false,"field":"COMPANY_ID"},{"type":"string","optional":false,"field":"COMPANY_PATH"},{"type":"string","optional":true,"field":"ACCT_AUTHENTIC"},{"type":"int64","optional":true,"field":"ACCT_INPUT_OCTETS"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_INTERIM_INTERVAL_DEADLINE"},{"type":"int64","optional":true,"field":"ACCT_OUTPUT_OCTETS"},{"type":"string","optional":false,"field":"ACCT_SESSION_ID"},{"type":"int32","optional":true,"field":"ACCT_SESSION_TIME"},{"type":"int32","optional":true,"field":"ACCT_START_DELAY"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_START_TIME"},{"type":"int32","optional":true,"field":"ACCT_STOP_DELAY"},{"type":"int64","optional":true,"name":"io.debezium.time.Timestamp","version":1,"field":"ACCT_STOP_TIME"},{"type":"string","optional":true,"field":"ACCT_TERMINATE_CAUSE"},{"type":"string","optional":true,"field":"ACCT_UNIQUE_ID"},{"type":"string","optional":true,"field":"CALLED_STATION_ID"},{"type":"string","optional":true,"field":"CALLING_STATION_ID"},{"type":"string","optional":true,"field":"CHARGE_ID"},{"type":"boolean","optional":true,"field":"CHARGED"},{"type":"string","optional":true,"field":"CONNECT_INFO_START"},{"type":"string","optional":true,"field":"CONNECT_INFO_STOP"},{"type":"string","optional":true,"field":"CUSTOMER_ID"},{"type":"string","optional":true,"field":"FRAMED_IP_ADDRESS"},{"type":"string","optional":true,"field":"FRAMED_PROTOCOL"},{"type":"string","optional":true,"field":"INTERNAL_SERVICE_TYPE"},{"type":"string","optional":true,"field":"NAS_IP_ADDRESS"},{"type":"string","optional":true,"field":"NAS_PORT_ID"},{"type":"string","optional":true,"field":"NAS_PORT_TYPE"},{"type":"string","optional":true,"field":"NETWORK_SPACE_ID"},{"type":"string","optional":true,"field":"REALM"},{"type":"string","optional":true,"field":"SERVICE_PROFILE"},{"type":"string","optional":true,"field":"SERVICE_TYPE"},{"type":"string","optional":true,"field":"STATUS"},{"type":"string","optional":true,"field":"STRIPPED_USERNAME"},{"type":"string","optional":true,"field":"USERNAME"},{"type":"string","optional":true,"field":"NAS_ID"},{"type":"string","optional":true,"field":"NAS_NETWORK_SPACE_ID"}],"optional":true,"name":"dbserver.test.RADIUS_ACCOUNTINGS_HISTORY.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"server_id"},{"type":"int64","optional":false,"field":"ts_sec"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"boolean","optional":true,"field":"snapshot"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"}],"optional":false,"name":"dbserver.test.RADIUS_ACCOUNTINGS_HISTORY.Envelope","version":1},"payload":{"before":null,"after":{"ID":"2017013112051810844165153","CREATED_BY":null,"DATE_ENTERED":1485864318000,"DATE_MODIFIED":1485864318000,"DELETED":false,"MODIFIED_BY":null,"COMPANY_ID":1300002,"COMPANY_PATH":"1.304","ACCT_AUTHENTIC":"RADIUS","ACCT_INPUT_OCTETS":6000,"ACCT_INTERIM_INTERVAL_DEADLINE":1485864318000,"ACCT_OUTPUT_OCTETS":0,"ACCT_SESSION_ID":"session2","ACCT_SESSION_TIME":244,"ACCT_START_DELAY":0,"ACCT_START_TIME":1485864318000,"ACCT_STOP_DELAY":0,"ACCT_STOP_TIME":1485864978000,"ACCT_TERMINATE_CAUSE":"User-Request","ACCT_UNIQUE_ID":"373d762676a67891","CALLED_STATION_ID":"1100002","CALLING_STATION_ID":"CCFA00E63BD6","CHARGE_ID":"SUCCESS","CHARGED":false,"CONNECT_INFO_START":null,"CONNECT_INFO_STOP":null,"CUSTOMER_ID":"ff808081545d935701555394c80118bf","FRAMED_IP_ADDRESS":"192.168.189.67","FRAMED_PROTOCOL":null,"INTERNAL_SERVICE_TYPE":"HOTSPOT","NAS_IP_ADDRESS":"192.168.105.140","NAS_PORT_ID":null,"NAS_PORT_TYPE":"Wireless-802.11","NETWORK_SPACE_ID":"1200002","REALM":null,"SERVICE_PROFILE":null,"SERVICE_TYPE":null,"STATUS":"SUCCESS","STRIPPED_USERNAME":"USERFOO@somemail.com","USERNAME":"USERFOO@somemail.com","NAS_ID":null,"NAS_NETWORK_SPACE_ID":"ff808081545d93570154cdd48c300d70"},"source":{"name":"dbserver","server_id":10010,"ts_sec":1485835518,"gtid":"b2506b1d-94c1-11e6-9bb3-1c1b0d26e666:1337","file":"mysql-bin.000105","pos":9580,"row":0,"snapshot":null},"op":"c","ts_ms":1485835518413}} """
  val hotspotStr = """  """
  val wropStr = """ {"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"string","optional":true,"field":"username"},{"type":"string","optional":true,"field":"company_id"},{"type":"string","optional":true,"field":"called_station_id"},{"type":"string","optional":true,"field":"nas_ip_address"},{"type":"string","optional":true,"field":"reply_message"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"field":"entered_date"},{"type":"string","optional":true,"field":"framed_ip_address"},{"type":"string","optional":true,"field":"result"},{"type":"string","optional":true,"field":"calling_station_id"},{"type":"string","optional":true,"field":"network_name"},{"type":"int64","optional":true,"field":"session_timeout"},{"type":"string","optional":true,"field":"network_space_id"}],"optional":true,"name":"dbserver.test.wrop_post_auth_results.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"string","optional":true,"field":"username"},{"type":"string","optional":true,"field":"company_id"},{"type":"string","optional":true,"field":"called_station_id"},{"type":"string","optional":true,"field":"nas_ip_address"},{"type":"string","optional":true,"field":"reply_message"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"field":"entered_date"},{"type":"string","optional":true,"field":"framed_ip_address"},{"type":"string","optional":true,"field":"result"},{"type":"string","optional":true,"field":"calling_station_id"},{"type":"string","optional":true,"field":"network_name"},{"type":"int64","optional":true,"field":"session_timeout"},{"type":"string","optional":true,"field":"network_space_id"}],"optional":true,"name":"dbserver.test.wrop_post_auth_results.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"server_id"},{"type":"int64","optional":false,"field":"ts_sec"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"boolean","optional":true,"field":"snapshot"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"}],"optional":false,"name":"dbserver.test.wrop_post_auth_results.Envelope","version":1},"payload":{"before":null,"after":{"id":20170201112640,"username":"USERFOO","company_id":"300002","called_station_id":"11:22:33:44:55:43","nas_ip_address":"127.0.0.1","reply_message":"SERVICE_AREA_NOT_FOUND","entered_date":"2017-02-01T03:11:53+08:00","framed_ip_address":"","result":"Access-Reject","calling_station_id":"41:28:83:53:93:92","network_name":"NetworkSpace not found (11:22:33:44:55:43)","session_timeout":0,"network_space_id":"200002"},"source":{"name":"dbserver","server_id":10010,"ts_sec":1485918713,"gtid":"b2506b1d-94c1-11e6-9bb3-1c1b0d26e666:1348","file":"mysql-bin.000106","pos":6393,"row":0,"snapshot":null},"op":"c","ts_ms":1485918713320}} """

  val radius = extractToAfter(radiusStr).extract[RadiusAccounts]
  val splash = extractToAfter(splashStr).extract[SplashPortal]
  // val hotspot = extractToAfter(hotspotStr).extract[Hotspots]
  val wrop = extractToAfter(wropStr).extract[WropAuthPost]

  println("Radius Accounting: " + radius)
  println("Splashportal: " + splash)
  // println("Hotspot: " + hotspot)
  println("Wrop Post Auth: " + wrop)
}