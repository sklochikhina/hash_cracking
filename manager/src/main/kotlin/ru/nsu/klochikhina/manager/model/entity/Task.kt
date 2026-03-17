package ru.nsu.klochikhina.manager.model.entity

import enums.TaskStatus
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.CompoundIndex
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document(collection = "tasks")
@CompoundIndex(name = "req_start_idx", def = "{'requestId': 1, 'startIndex': 1}", unique = true)
data class Task (
    @Id val id: String? = null,
    val requestId: String,
    val startIndex: Long,
    val count: Long,
    val targetHash: String,
    val maxLength: Int,
    val status: TaskStatus = TaskStatus.QUEUED,

    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
)
