package demo

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.io.Source

object ScalaDemo {
    def main(args: Array[String]): Unit = {
        val month = LocalDate.now()//
        for (n <- 0 to 48) {
            println(month.minusMonths(n).format(DateTimeFormatter.ofPattern("yyyy-MM")))
        }
    }





    def regex(): Unit = {
        /** 正则表达式匹配手机号 */
        println("13012345678".matches("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$"))
    }


    def float(): Unit = {
        //避免使用发float或double表示0.1或者非负次方值
        println(1.00 - 9 * 0.10) //0.09999999999999998
    }


    def httpRequest(): Unit = {
        val reportUrl = "http://host/20180307/xxxxxxxxxx.txt"
        val start = System.currentTimeMillis()
        val s = Source.fromURL(reportUrl).mkString
        val end = System.currentTimeMillis()
        println((end - start))
        //println(s)
    }
}

