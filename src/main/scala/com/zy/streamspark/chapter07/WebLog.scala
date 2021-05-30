package com.zy.streamspark.chapter07

import java.sql.Timestamp

case class WebLog(
                 host:String,
                 timestamp:Timestamp,
                 request:String,
                 http_reply:Int,
                 bytes:Long
                 ) extends java.io.Serializable
