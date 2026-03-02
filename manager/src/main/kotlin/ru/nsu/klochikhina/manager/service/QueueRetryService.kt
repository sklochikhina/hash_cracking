package ru.nsu.klochikhina.manager.service

import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.QueuedTask
import ru.nsu.klochikhina.manager.model.entity.QueuedTaskStatus
import ru.nsu.klochikhina.manager.repository.QueuedTaskRepository
import java.time.Instant

@Service
class QueueRetryService(
    private val mongoTemplate: MongoTemplate,
    private val queuedTaskRepository: QueuedTaskRepository,
    private val rabbitTemplate: RabbitTemplate
) {

    private val logger = org.slf4j.LoggerFactory.getLogger(QueueRetryService::class.java)

    @Scheduled(fixedDelayString = $$"${app.queue.retry.delay-ms:10000}")
    fun processQueuedTasksScheduled() {
        processQueuedOnce()
    }

    fun processQueuedNow() {
        processQueuedOnce()
    }

    private fun processQueuedOnce() {
        while (true) {
            val task = claimOneQueuedTask() ?: break
            try {
                sendQueuedTask(task)
                queuedTaskRepository.deleteById(task.id!!)
                logger.info("Queued task ${task.taskId} sent and removed")
            } catch (ex: Exception) {
                logger.error("Failed to send queued task ${task.taskId}", ex)
                val attempts = task.attempts + 1
                val updated = task.copy(
                    attempts = attempts,
                    lastAttemptAt = Instant.now(),
                    status = if (attempts >= task.maxAttempts) QueuedTaskStatus.ERROR else QueuedTaskStatus.QUEUED
                )
                queuedTaskRepository.save(updated)
            }
        }
    }

    private fun sendQueuedTask(task: QueuedTask) {
        rabbitTemplate.convertAndSend(
            task.exchange,
            task.routingKey,
            task.payloadJson
        ) { msg ->
            msg.messageProperties.deliveryMode = MessageDeliveryMode.PERSISTENT
            msg.messageProperties.contentType = "application/json"
            msg.messageProperties.contentEncoding = "utf-8"
            msg
        }
    }

    private fun claimOneQueuedTask(): QueuedTask? {
        val query = Query(Criteria.where("status").`is`(QueuedTaskStatus.QUEUED.name))
        val update = Update()
            .set("status", QueuedTaskStatus.SENDING.name)
            .set("lastAttemptAt", Instant.now())
        val option = FindAndModifyOptions.options().returnNew(true)
        return mongoTemplate.findAndModify<QueuedTask>(query, update, option)
    }
}