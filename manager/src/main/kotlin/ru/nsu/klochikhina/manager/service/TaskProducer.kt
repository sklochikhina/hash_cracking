package ru.nsu.klochikhina.manager.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dto.TaskDto
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import ru.nsu.klochikhina.manager.model.entity.QueuedTask
import ru.nsu.klochikhina.manager.repository.QueuedTaskRepository
import java.util.UUID

@Service
class TaskProducer(
    private val rabbitTemplate: RabbitTemplate,
    private val queuedTaskRepository: QueuedTaskRepository
) {

    private val logger = LoggerFactory.getLogger(TaskProducer::class.java)
    private val mapper = jacksonObjectMapper().registerKotlinModule()

    fun sendOrQueueTask(task: TaskDto) {
        val json = mapper.writeValueAsString(task)
        try {
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
        } catch (e: Exception) {
            val qt = QueuedTask(
                id = UUID.randomUUID().toString(),
                taskId = task.taskId,
                requestId = task.requestId,
                payloadJson = json,
                routingKey = RabbitConstants.TASK_ROUTING_KEY,
                exchange = RabbitConstants.TASK_EXCHANGE
            )
            queuedTaskRepository.save(qt)
            logger.warn("Rabbit unavailable — queued task ${task.taskId} (request ${task.requestId}) : ${e.message}")
        }
    }
}