package ru.nsu.klochikhina.manager.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dto.TaskDto
import enums.TaskStatus
import org.slf4j.LoggerFactory
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
import ru.nsu.klochikhina.manager.model.entity.Task
import ru.nsu.klochikhina.manager.repository.TaskRepository
import rabbit.RabbitConstants
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

@Service
class OutboxDispatcher(
    private val mongoTemplate: MongoTemplate,
    private val taskRepository: TaskRepository,
    private val rabbitTemplate: RabbitTemplate
) {
    private val logger = LoggerFactory.getLogger(OutboxDispatcher::class.java)

    private val running = AtomicBoolean(false)
    private val mapper = jacksonObjectMapper().registerKotlinModule()

    private var batchSize: Int = 100

    @Scheduled(fixedDelayString = "\${app.queue.retry.delay-ms:10000}")
    fun processQueuedTasksScheduled() {
        processQueuedOnce()
    }

    @Async("taskExecutor")
    fun processQueuedNow() {
        processQueuedOnce()
    }

    private fun processQueuedOnce() {
        if (!running.compareAndSet(false, true)) {
            logger.info("OutboxDispatcher already running — skipping")
            return
        }

        logger.info("OutboxDispatcher: start processing queued tasks")
        var processed = 0
        try {
            while (processed < batchSize) {
                val task = claimOneQueuedTask() ?: break
                try {
                    sendTask(task)
                    taskRepository.findById(task.id!!).ifPresent { t ->
                        taskRepository.save(t.copy(status = TaskStatus.SENT, updatedAt = Instant.now()))
                    }
                    logger.info("Queued task ${task.id} sent and marked SENT")
                    processed++
                } catch (e: Exception) {
                    logger.warn("Failed to send queued task ${task.id}: ${e.message}")
                    taskRepository.findById(task.id!!).ifPresent { t ->
                        taskRepository.save(t.copy(status = TaskStatus.QUEUED, updatedAt = Instant.now()))
                    }
                    Thread.sleep(200)
                }
            }
        } finally {
            running.set(false)
            logger.info("OutboxDispatcher: finished, processed=$processed")
        }
    }

    private fun sendTask(t: Task) {
        val dto = TaskDto(
            taskId = t.id ?: throw IllegalStateException("Task.id is null"),
            requestId = t.requestId,
            startIndex = t.startIndex,
            count = t.count,
            targetHash = t.targetHash,
            maxLength = t.maxLength
        )

        val json = mapper.writeValueAsString(dto)
        rabbitTemplate.convertAndSend(
            RabbitConstants.TASK_EXCHANGE,
            RabbitConstants.TASK_ROUTING_KEY,
            json
        ) { msg ->
            msg.messageProperties.deliveryMode = MessageDeliveryMode.PERSISTENT
            msg.messageProperties.contentType = "application/json"
            msg.messageProperties.contentEncoding = "UTF-8"
            msg
        }
    }

    private fun claimOneQueuedTask(): Task? {
        val query = Query(Criteria.where("status").`is`(TaskStatus.QUEUED.name))
            .with(Sort.by(Sort.Direction.ASC, "createdAt"))
            .limit(1)
        val update = Update().set("updatedAt", Instant.now())
        val option = FindAndModifyOptions.options().returnNew(true)

        return mongoTemplate.findAndModify<Task>(query, update, option)
    }
}
