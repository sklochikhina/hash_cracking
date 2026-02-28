package ru.nsu.klochikhina.manager.service

import enums.RequestStatus
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.repository.RequestRepository

@Service
class HashService(
    private val requestRepository: RequestRepository
) {

    fun createRequest(hash: String, maxLength: Int): String {
        val request = HashRequest(
            hash = hash,
            maxLength = maxLength,
            status = RequestStatus.PENDING,
        )
        return requestRepository.save(request).id!!
    }

    fun getStatus(requestId: String): HashRequest {
        return requestRepository.findById(requestId)
            .orElseThrow {
                RuntimeException("Request with id $requestId not found")
            }
    }
}