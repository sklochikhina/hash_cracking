package ru.nsu.klochikhina.manager.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.Channel
import dto.ResultDto
import enums.RequestStatus
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.dao.OptimisticLockingFailureException
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.repository.RequestRepository
import java.nio.charset.StandardCharsets

@Service
class ResultListener(
    private val requestRepository: RequestRepository
) {

    private val logger = LoggerFactory.getLogger(ResultListener::class.java)
    private val mapper = jacksonObjectMapper().registerKotlinModule()

    @RabbitListener(
        queues = [RabbitConstants.RESULT_QUEUE],
        containerFactory = "rabbitListenerContainerFactory"
    )
    fun handleResult(message: Message, channel: Channel) {
        val deliveryTag = message.messageProperties.deliveryTag
        try {
            val body = String(message.body, StandardCharsets.UTF_8)
            val resultMsg = mapper.readValue<ResultDto>(body)

            try {
                applyResultWithRetry(resultMsg, maxRetries = 5)
                channel.basicAck(deliveryTag, false)
            } catch (e: Exception) {
                logger.error("Couldn't save the result for the request=${resultMsg.requestId} after retries", e)
                channel.basicNack(deliveryTag, false, false)
            }

        } catch (e: Exception) {
            logger.error("Unexpected error in handleResult", e)
            channel.basicNack(deliveryTag, false, true)
        }
    }

    fun applyResultWithRetry(result: ResultDto, maxRetries: Int = 5) {
        var attempt = 0
        while (true) {
            attempt++
            try {
                val opt = requestRepository.findById(result.requestId)
                if (!opt.isPresent) {
                    logger.warn("Request ${result.requestId} not found (skipping result)")
                    return
                }

                val req: HashRequest = opt.get()

                val newResults = if (result.results.isNotEmpty()) req.results + result.results else req.results
                val newCompleted = req.completedTasks + 1L
                val newStatus = when {
                    newResults.isNotEmpty() -> RequestStatus.READY
                    req.totalTasks in 1..newCompleted -> RequestStatus.ERROR
                    else -> RequestStatus.IN_PROGRESS
                }

                val updated = req.copy(
                    results = newResults,
                    completedTasks = newCompleted,
                    status = newStatus
                )

                requestRepository.save(updated)
                return

            } catch (e: OptimisticLockingFailureException) {
                if (attempt >= maxRetries) {
                    logger.error("Optimistic lock retry limit reached for request=${result.requestId}, attempts=$attempt")
                    throw e
                }
                val backoff = 50L * attempt
                logger.warn("Optimistic lock conflict for request=${result.requestId}, retry #$attempt after ${backoff}ms")
                Thread.sleep(backoff)
            }
        }
    }
}