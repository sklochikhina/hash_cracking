package ru.nsu.klochikhina.manager.controller

import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.validation.FieldError
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class GlobalExceptionHandler {
    private val logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleValidation(ex: MethodArgumentNotValidException): ResponseEntity<Map<String, Any>> {
        val errors = ex.bindingResult.fieldErrors.associate { (it as FieldError).field to it.defaultMessage.orEmpty() }
        logger.info("Validation failed: $errors")
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(mapOf("error" to "Validation failed", "fields" to errors))
    }

    @ExceptionHandler(IllegalArgumentException::class)
    fun handleBadRequest(ex: IllegalArgumentException): ResponseEntity<Map<String, String>> {
        logger.info("Bad request: ${ex.message}")
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(mapOf("error" to (ex.message ?: "Bad request")))
    }

    @ExceptionHandler(DuplicateKeyException::class)
    fun handleConflict(ex: DuplicateKeyException): ResponseEntity<Map<String, String>> {
        logger.info("Conflict: ${ex.message}")
        return ResponseEntity.status(HttpStatus.CONFLICT).body(mapOf("error" to "Resource already exists"))
    }

    @ExceptionHandler(Exception::class)
    fun handleAll(ex: Exception): ResponseEntity<Map<String, String>> {
        logger.error("Internal error: ${ex.message}", ex)
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mapOf("error" to "Internal server error"))
    }
}
