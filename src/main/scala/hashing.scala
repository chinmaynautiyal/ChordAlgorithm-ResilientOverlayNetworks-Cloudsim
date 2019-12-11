package com.nautiyalraj

import java.security.MessageDigest
import java.lang.Long

import org.apache.commons.codec.binary.Hex


object hashing {

  var HASH_TYPE = "SHA-1"

  def getHash(id: String, chordSpace: Int): Int = {

    if (id != null) {
      var key = MessageDigest.getInstance(HASH_TYPE).digest(id.getBytes("UTF-8")).map("%02X" format _).mkString.trim()
      if (key.length() > 15) {
        key = key.substring(key.length() - 15);
      }
      (Long.parseLong(key, 16) % chordSpace).toInt
    } else
      0
  }

  def hashKey(ID: String): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val hexString = new StringBuffer();
    val stringwithseed = ID
    sha.update(stringwithseed.getBytes("UTF-8"))
    var digest = sha.digest();
    var digestj = java.util.Arrays.copyOf(digest, 1 / 8)
    Hex.encodeHexString(digestj)

  }
}