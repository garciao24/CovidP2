package com
object PasswordEncryption{
  def md5Hash(text: String) : String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map
  { "%02x".format(_) }.foldLeft(""){_ + _}
 
  
  def main(args: Array[String]): Unit = {
 println(("This is Hashed Password : " + md5Hash("testPassword"  )))
   }
}
