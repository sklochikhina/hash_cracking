package ru.nsu.klochikhina.worker.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.Channel
import constants.Constants
import dto.ResultDto
import dto.TaskDto
import enums.ResultStatus
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

@Service
class WorkerListener(private val rabbitTemplate: RabbitTemplate) {

    companion object {
        private val logger = LoggerFactory.getLogger(WorkerListener::class.java)
    }

    private val mapper = jacksonObjectMapper().registerKotlinModule()
    private val alphabet = Constants.ALPHABET.toCharArray()
    private val base = Constants.BASE

    @RabbitListener(
        queues = [RabbitConstants.TASK_QUEUE],
        containerFactory = "rabbitListenerContainerFactory"
    )
    fun handleMessage(message: Message, channel: Channel) {
        val deliveryTag = message.messageProperties.deliveryTag
        try {
            val body = String(message.body, StandardCharsets.UTF_8)
            val task = mapper.readValue<TaskDto>(body)

            val results = mutableListOf<String>()
            val end = task.startIndex + task.count

            for (i in task.startIndex until end) {
                val candidate = indexToString(i, task.maxLength)
                if (md5Hex(candidate).equals(task.targetHash, ignoreCase = true)) {
                    results.add(candidate)
                    break
                }
            }

            // TODO("error же отправляем, если не нашли подходящей строки?")
            val resultDto = ResultDto(
                taskId = task.taskId,
                requestId = task.requestId,
                results = results,
                status = if (results.isNotEmpty()) ResultStatus.DONE else ResultStatus.ERROR
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

    private fun md5Hex(input: String): String {
        val md = MessageDigest.getInstance("MD5")
        val bytes = md.digest(input.toByteArray(StandardCharsets.UTF_8))
        return bytes.joinToString("") { "%02x".format(it) }
    }

    private fun indexToString(index: Long, maxLength: Int): String {
        var rem = index
        var len = 1

        var i = 1
        while (i <= maxLength) {
            val block = pow(base, i) // base^i
            if (rem < block) {
                len = i
                break
            } else {
                rem -= block
            }
            i++
        }

        val chars = CharArray(len)
        var value = rem
        for (pos in (len - 1) downTo 0) {
            val digit = (value % base).toInt()
            chars[pos] = alphabet[digit]
            value /= base
        }
        return String(chars)
    }

    private fun pow(a: Long, exp: Int): Long {
        var r = 1L
        repeat(exp) { r *= a }
        return r
    }
}