package ru.nsu.klochikhina.manager.service

import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.QueuedTask
import ru.nsu.klochikhina.manager.model.entity.QueuedTaskStatus
import ru.nsu.klochikhina.manager.repository.QueuedTaskRepository
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

@Service
class QueueRetryService(
    private val mongoTemplate: MongoTemplate,
    private val queuedTaskRepository: QueuedTaskRepository,
    private val rabbitTemplate: RabbitTemplate
) {

    private val logger = org.slf4j.LoggerFactory.getLogger(QueueRetryService::class.java)
    private val running = AtomicBoolean(false)

    @Scheduled(fixedDelayString = $$"${app.queue.retry.delay-ms:10000}")
    fun processQueuedTasksScheduled() {
        processQueuedOnce()
    }

    @Async("taskExecutor")
    fun processQueuedNow() {
        processQueuedOnce()
    }

    private fun processQueuedOnce() {
        if (!running.compareAndSet(false, true)) {
            logger.info("processQueuedOnce already running — skipping")
            return
        }

        logger.info("QueueRetryService: start processing queued tasks")
        var processed = 0
        try {
            while (true) {
                val task = claimOneQueuedTask() ?: break
                try {
                    logger.info("Claimed queued task id=${task.id}, taskId=${task.taskId}, attempts=${task.attempts}")
                    sendQueuedTask(task)
                    task.id?.let { queuedTaskRepository.deleteById(it) }
                    logger.info("Queued task ${task.taskId} sent and removed")
                    processed++
                } catch (e: Exception) {
                    logger.warn("Failed to send queued task ${task.taskId}: ${e.message}")

                    val updated = task.copy(
                        lastAttemptAt = Instant.now(),
                        status = QueuedTaskStatus.QUEUED
                    )
                    queuedTaskRepository.save(updated)

                    Thread.sleep(200)
                }
            }
        } finally {
            running.set(false)
            logger.info("QueueRetryService: finished, processed=$processed")
        }
    }

    private fun sendQueuedTask(qt: QueuedTask) {
        rabbitTemplate.convertAndSend(
            qt.exchange,
            qt.routingKey,
            qt.payloadJson
        ) { msg ->
            msg.messageProperties.deliveryMode = MessageDeliveryMode.PERSISTENT
            msg.messageProperties.contentType = "application/json"
            msg.messageProperties.contentEncoding = "utf-8"
            msg
        }
    }

    private fun claimOneQueuedTask(): QueuedTask? {
        val query = Query(Criteria.where("status").`is`(QueuedTaskStatus.QUEUED.name))
            .with(Sort.by(Sort.Direction.ASC, "createdAt"))
            .limit(1)
        val update = Update()
            .set("status", QueuedTaskStatus.SENDING.name)
            .set("lastAttemptAt", Instant.now())
        val option = FindAndModifyOptions.options().returnNew(true)
        return mongoTemplate.findAndModify<QueuedTask>(query, update, option)
    }
}