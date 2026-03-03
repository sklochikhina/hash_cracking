package ru.nsu.klochikhina.manager.model.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document(collection = "queued_tasks")
data class QueuedTask (
    @Id val id: String? = null,
    val taskId: String,
    val requestId: String,
    val payloadJson: String,    // сериализованный TaskDto (JSON)
    val routingKey: String,
    val exchange: String,
    val maxAttempts: Int = 5,
    val attempts: Int = 0,
    val status: QueuedTaskStatus = QueuedTaskStatus.QUEUED,
    val createdAt: Instant = Instant.now(),
    val lastAttemptAt: Instant? = null
)

enum class QueuedTaskStatus {
    QUEUED,
    SENDING,
    ERROR
}