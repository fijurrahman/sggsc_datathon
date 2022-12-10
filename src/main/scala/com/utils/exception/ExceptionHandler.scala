package com.utils.exception

object ExceptionHandler {
  final case class BatchException(private val message: String = "")
    extends Exception(message)
}
