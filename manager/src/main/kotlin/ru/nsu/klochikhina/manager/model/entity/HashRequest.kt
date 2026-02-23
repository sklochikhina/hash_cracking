package ru.nsu.klochikhina.manager.model.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import ru.nsu.klochikhina.manager.enums.RequestStatus

@Document(collection = "hash_requests")
data class HashRequest(
    @Id
    val id: String? = null,
    val hash: String,
    val maxLength: Int,
    val status: RequestStatus,
    val result: String? = null
)
