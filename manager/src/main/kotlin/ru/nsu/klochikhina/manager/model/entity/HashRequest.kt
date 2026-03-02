package ru.nsu.klochikhina.manager.model.entity

import enums.RequestStatus
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.mapping.Document

@Document(collection = "hash_requests")
data class HashRequest(
    @Id val id: String? = null,
    val hash: String,
    val maxLength: Int,
    val status: RequestStatus = RequestStatus.PENDING,
    val results: List<String> = emptyList(),
    val totalTasks: Long = 0L,
    val completedTasks: Long = 0L, //
    @Version val version: Long? = null
)
