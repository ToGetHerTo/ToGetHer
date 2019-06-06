package com.chenyuxin

case class MyCaseClass(
                   mid:String,
                   uid:String,
                   appid: String,
                   area:String,
                   os:String,
                   ch:String,
                   logType:String,
                   vs:String,
                   //为了es查询方便
                   var logDate:String,
                   var logHour:String,
                   var logHourMinute:String,
                   val ts:Long
                   ){

}
