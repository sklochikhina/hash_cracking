package ru.nsu.klochikhina.worker.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.Channel
import dto.ResultDto
import dto.TaskDto
import enums.WorkerResultStatus
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import java.nio.charset.StandardCharsets

@Service
class WorkerListener(
    private val rabbitTemplate: RabbitTemplate,
    private val bruteForce: BruteForce
) {

    private val logger = LoggerFactory.getLogger(WorkerListener::class.java)
    private val mapper = jacksonObjectMapper().registerKotlinModule()

    @RabbitListener(
        queues = [RabbitConstants.TASK_QUEUE],
        containerFactory = "rabbitListenerContainerFactory"
    )
    fun handleMessage(message: Message, channel: Channel) {
        val deliveryTag = message.messageProperties.deliveryTag
        try {
            val body = String(message.body, StandardCharsets.UTF_8)
            val task = mapper.readValue<TaskDto>(body)

            val results = bruteForce.findFirstMatch(task)

            val resultDto = ResultDto(
                taskId = task.taskId,
                requestId = task.requestId,
                results = results,
                status = WorkerResultStatus.DONE // DONE = "задача обработана"
            )

            val json = mapper.writeValueAsString(resultDto)
            rabbitTemplate.convertAndSend(
                RabbitConstants.RESULT_EXCHANGE,
                RabbitConstants.RESULT_ROUTING_KEY,
                json
            ) { msg ->
                msg.messageProperties.deliveryMode = MessageDeliveryMode.PERSISTENT
                msg.messageProperties.contentType = "application/json"
                msg.messageProperties.contentEncoding = "UTF-8"
                msg
            }
            channel.basicAck(deliveryTag, false)

        } catch (e: Exception) {
            logger.error("Error processing task", e)
            channel.basicNack(deliveryTag, false, true)
        }
    }
}
