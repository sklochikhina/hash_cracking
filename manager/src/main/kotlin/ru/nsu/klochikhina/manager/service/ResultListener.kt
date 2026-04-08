package ru.nsu.klochikhina.manager.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.Channel
import dto.ResultDto
import enums.TaskStatus
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
import org.springframework.stereotype.Service
import rabbit.RabbitConstants
import ru.nsu.klochikhina.manager.model.entity.Task
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
                markTaskDone(resultMsg)
                channel.basicAck(deliveryTag, false)
            } catch (e: Exception) {
                logger.error("Couldn't process result for request=${resultMsg.requestId}", e)
                channel.basicNack(deliveryTag, false, true)
            }
        } catch (e: Exception) {
            logger.error("Unexpected error in handleResult", e)
            channel.basicNack(deliveryTag, false, true)
        }
    }

    private fun markTaskDone(result: ResultDto, maxRetries: Int = 5) {
        var attempt = 0

        while (true) {
            attempt++
            try {
                val query = Query(
                    Criteria.where("_id").`is`(result.taskId)
                        .and("status").ne(TaskStatus.DONE)
                )

                val update = Update()
                    .set("status", TaskStatus.DONE)
                    .addToSet("resultset").each(*result.results.toTypedArray())

                val options = FindAndModifyOptions.options().returnNew(true)

                val updated = mongoTemplate.findAndModify<Task>(query, update, options)
                if (updated == null) {
                    logger.info("Task ${result.taskId} already DONE, skipping")
                }
                return

            } catch (e: OptimisticLockingFailureException) {
                if (attempt >= maxRetries) throw e
                Thread.sleep(50L * attempt)

            } catch (e: Exception) {
                if (attempt >= maxRetries) throw e
                Thread.sleep(50L * attempt)
            }
        }
    }
}
