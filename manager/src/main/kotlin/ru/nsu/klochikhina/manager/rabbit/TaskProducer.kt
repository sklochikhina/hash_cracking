package ru.nsu.klochikhina.manager.rabbit

import org.springframework.stereotype.Service
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.core.RabbitTemplate
import dto.TaskDto
import rabbit.RabbitConstants

@Service
class TaskProducer(private val rabbitTemplate: RabbitTemplate) {

    private val mapper = jacksonObjectMapper().registerKotlinModule()

    fun sendTask(task: TaskDto) {
        val json = mapper.writeValueAsString(task)
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
}