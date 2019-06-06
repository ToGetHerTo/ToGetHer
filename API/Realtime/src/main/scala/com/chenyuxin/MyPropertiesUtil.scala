package com.chenyuxin

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author chenshiliu
  * @create 2019-06-05 16:40
  *
  * 读取配置文件的工具类
  */
object MyPropertiesUtil {

//  //测试
//  def main(args: Array[String]): Unit = {
//    val properties: Properties = MyPropertiesUtil.load("config.properties")
//    println(properties.getProperty("kafka.broker.list"))
//  }

  def load(propertieName:String): Properties ={
    val properties = new Properties();
    properties.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) ,
        "UTF-8")
    )
    properties
  }
}
