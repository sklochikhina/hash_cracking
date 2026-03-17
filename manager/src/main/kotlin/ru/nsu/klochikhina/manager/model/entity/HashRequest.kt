package ru.nsu.klochikhina.manager.model.entity

import enums.RequestStatus
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import org.springframework.data.mongodb.core.index.CompoundIndex
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document(collection = "hash_requests")
@CompoundIndex(name = "hash_maxlen_idx", def = "{'hash': 1, 'maxLength': 1}", unique = true)
data class HashRequest(
    @Id val id: String? = null,
    val hash: String,
    val maxLength: Int,
    val status: RequestStatus = RequestStatus.PENDING,
    val results: List<String> = emptyList(),
    val totalTasks: Long = 0L,
    val completedTasks: Long = 0L,
    val lastProcessedIndex: Long = 0L,
    @Version val version: Long? = null,

    val lockedBy: String? = null,
    val lockUntil: Instant? = null
)
