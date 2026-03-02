package ru.nsu.klochikhina.manager.service

import enums.RequestStatus
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.repository.RequestRepository
import java.util.UUID

@Service
class HashService(
    private val requestRepository: RequestRepository,
    private val taskSplitter: TaskSplitter,
) {

    fun createRequest(hash: String, maxLength: Int): String {
        val request = HashRequest(
            id = UUID.randomUUID().toString(),
            hash = hash,
            maxLength = maxLength,
            status = RequestStatus.PENDING,
        )
        val saved = requestRepository.save(request)

        taskSplitter.splitAndSendAsync(request.id!!, hash, maxLength)

        return saved.id!!
    }

    fun getStatus(requestId: String): HashRequest {
        return requestRepository.findById(requestId)
            .orElseThrow {
                RuntimeException("Request with id $requestId not found")
            }
    }
}