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
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.updateFirst
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import java.nio.charset.StandardCharsets

@Service
class ResultListener(
    private val mongoTemplate: MongoTemplate
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
                applyResultAtomicallyWithRetry(resultMsg, maxRetries = 5)
                channel.basicAck(deliveryTag, false)
            } catch (e: Exception) {
                logger.error("Couldn't save the result for the request=${resultMsg.requestId} after retries", e)
                channel.basicNack(deliveryTag, false, true)
            }

        } catch (e: Exception) {
            logger.error("Unexpected error in handleResult", e)
            channel.basicNack(deliveryTag, false, true)
        }
    }

    fun applyResultAtomicallyWithRetry(result: ResultDto, maxRetries: Int = 5) {
        var attempt = 0
        while (true) {
            attempt++
            try {
                val id = result.requestId

                val query = Query(Criteria.where("_id").`is`(id))
                val update = Update().inc("completedTasks", 1L)

                if (result.results.isNotEmpty()) {
                    if (result.results.size == 1) {
                        update.addToSet("results", result.results[0])
                    } else {
                        update.addToSet("results").each(*result.results.toTypedArray())
                    }
                }

                val options = FindAndModifyOptions.options().returnNew(true)
                val updated: HashRequest? = mongoTemplate.findAndModify<HashRequest>(query, update, options)

                if (updated == null) {
                    logger.warn("Request ${result.requestId} not found when applying result - skipping")
                    return
                }

                if (updated.results.isNotEmpty()) {
                    val qReady = Query(
                        Criteria.where("_id").`is`(id)
                            .and("status").ne(RequestStatus.READY)
                    )
                    val setReady = Update().set("status", RequestStatus.READY)
                    mongoTemplate.updateFirst<HashRequest>(qReady, setReady)
                    logger.info("Request $id marked READY (result found).")
                    return
                }

                if (updated.totalTasks > 0 && updated.completedTasks >= updated.totalTasks) {
                    val qError = Query(
                        Criteria.where("_id").`is`(id)
                            .and("completedTasks").gte(updated.totalTasks)
                            .and("results").size(0)
                            .and("status").ne(RequestStatus.READY)
                    )
                    val setError = Update().set("status", RequestStatus.ERROR)
                    val res = mongoTemplate.findAndModify<HashRequest>(
                        qError,
                        setError,
                        FindAndModifyOptions.options().returnNew(true)
                    )
                    if (res != null) {
                        logger.info("Request $id marked ERROR (all tasks finished, no results).")
                    } else {
                        logger.info("Request $id: attempted to mark ERROR but condition didn't match (likely READY created concurrently).")
                    }
                    return
                }

                return

            } catch (e: OptimisticLockingFailureException) {
                if (attempt >= maxRetries) {
                    logger.error("Optimistic lock retry limit reached for request=${result.requestId}, attempts=$attempt")
                    throw e
                }
                val backoff = 50L * attempt
                logger.warn("Optimistic lock conflict for request=${result.requestId}, retry #$attempt after ${backoff}ms")
                Thread.sleep(backoff)

            } catch (e: Exception) {
                if (attempt >= maxRetries) {
                    logger.error("applyResult failed for request=${result.requestId} after $attempt attempts", e)
                    throw e
                }
                val backoff = 50L * attempt
                logger.warn("Transient error applying result for request=${result.requestId}, retry #$attempt after ${backoff}ms: ${e.message}")
                Thread.sleep(backoff)

            }
        }
    }
}
