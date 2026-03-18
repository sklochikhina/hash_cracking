package ru.nsu.klochikhina.manager.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dto.TaskDto
import enums.TaskStatus
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import ru.nsu.klochikhina.manager.model.entity.Task
import ru.nsu.klochikhina.manager.repository.TaskRepository
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Service
class OutboxDispatcher(
    private val mongoTemplate: MongoTemplate,
    private val taskRepository: TaskRepository,
    private val rabbitTemplate: RabbitTemplate
) {
    private val logger = LoggerFactory.getLogger(OutboxDispatcher::class.java)
    private val mapper = jacksonObjectMapper().registerKotlinModule()

    private var pollSleepMs: Long = 200L
    private var errorSleepMs: Long = 2000L

    private var batchSize: Int = 200

    private var leaseSec: Long = 30L
    private var reaperDelayMs: Long = 10000L

    private val running = AtomicBoolean(false)
    private val stopped = AtomicBoolean(false)
    private val executor = Executors.newSingleThreadExecutor { r ->
        Thread(r, "outbox-dispatcher-thread").apply { isDaemon = true }
    }

    @PostConstruct
    fun start() {
        if (!running.compareAndSet(false, true)) return
        executor.submit { runLoop() }
        logger.info("OutboxDispatcher started")
    }

    @PreDestroy
    fun stop() {
        stopped.set(true)
        executor.shutdownNow()
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        logger.info("OutboxDispatcher stopped")
    }

    private fun runLoop() {
        while (!stopped.get()) {
            try {
                var processed = 0
                while (processed < batchSize && !stopped.get()) {
                    val task = claimOneQueuedTask() ?: break
                    try {
                        sendTaskToRabbit(task)
                        taskRepository.findById(task.id!!).ifPresent { t ->
                            taskRepository.save(
                                t.copy(
                                    status = TaskStatus.SENT,
                                    updatedAt = Instant.now()
                                )
                            )
                        }
                        processed++

                    } catch (e: Exception) {
                        logger.warn("Failed to send queued task ${task.id}: ${e.message}")
                        taskRepository.findById(task.id!!).ifPresent { t ->
                            taskRepository.save(t.copy(status = TaskStatus.QUEUED, updatedAt = Instant.now()))
                        }

                        Thread.sleep(errorSleepMs)
                        break
                    }
                }

                if (processed == 0) {
                    Thread.sleep(pollSleepMs)
                }

            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            } catch (t: Throwable) {
                logger.error("OutboxDispatcher main loop error: ${t.message}", t)
                try {
                    Thread.sleep(errorSleepMs)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
            }
        }
    }

    private fun sendTaskToRabbit(t: Task) {
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
        val now = Instant.now()

        val query = Query(Criteria.where("status").`is`(TaskStatus.QUEUED.name))
            .with(Sort.by(Sort.Direction.ASC, "createdAt"))
            .limit(1)

        val update = Update()
            .set("status", TaskStatus.SENDING.name)
            .set("updatedAt", now)

        val option = FindAndModifyOptions.options().returnNew(true)
        return try {
            mongoTemplate.findAndModify<Task>(query, update, option)
        } catch (e: Exception) {
            logger.error("Error claiming queued task: ${e.message}", e)
            null
        }
    }

    @Scheduled(fixedDelay = 10000L)
    fun requeueExpiredSendingTasks() {
        try {
            val cutoff = Instant.now().minusSeconds(leaseSec)
            val query = Query(
                Criteria.where("status").`is`(TaskStatus.SENDING.name)
                    .and("updatedAt").lte(cutoff)
            ).limit(200)

            val expired = mongoTemplate.find<Task>(query)
            if (expired.isEmpty()) return

            logger.info("Reaper: found ${expired.size} expired SENDING tasks, requeueing up to 200")

            for (t in expired) {
                try {
                    val q = Query(
                        Criteria.where("_id").`is`(t.id)
                            .and("status").`is`(TaskStatus.SENDING.name)
                    )
                    val u = Update()
                        .set("status", TaskStatus.QUEUED.name)
                        .set("updatedAt", Instant.now())
                    val opt = FindAndModifyOptions.options().returnNew(true)
                    val res = mongoTemplate.findAndModify<Task>(q, u, opt)

                    if (res != null) {
                        logger.info("Requeued expired task ${t.id}")
                    } else {
                        logger.debug("Reaper: task ${t.id} status changed concurrently, skipping")
                    }

                } catch (e: Exception) {
                    logger.warn("Reaper: failed to requeue task ${t.id}: ${e.message}")
                }
            }
        } catch (e: Exception) {
            logger.error("Reaper error: ${e.message}", e)
        }
    }
}
