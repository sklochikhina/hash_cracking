package ru.nsu.klochikhina.manager.service

import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.repository.RequestRepository
import java.util.UUID

@Service
class HashService(
    private val requestRepository: RequestRepository,
) {
    private val logger = LoggerFactory.getLogger(HashService::class.java)

    fun createRequest(hash: String, maxLength: Int): String {
        requestRepository.findFirstByHashAndMaxLength(hash, maxLength)?.let { existing ->
            return existing.id!!
        }

        val request = HashRequest(
            id = UUID.randomUUID().toString(),
            hash = hash,
            maxLength = maxLength
        )

        return try {
            val saved = requestRepository.save(request)
            saved.id!!
        } catch (e: DuplicateKeyException) {
            logger.info("Request for hash already created concurrently; reading existing entry")
            requestRepository.findFirstByHashAndMaxLength(hash, maxLength)
                ?.id
                ?: throw RuntimeException("Request created concurrently but cannot be read", e)
        } catch (e: Exception) {
            logger.error("Failed to create request for hash=$hash maxLength=$maxLength: ${e.message}", e)
            throw e
        }
    }

    fun getStatus(requestId: String): HashRequest {
        return requestRepository.findById(requestId)
            .orElseThrow {
                RuntimeException("Request with id $requestId not found")
            }
    }
}
