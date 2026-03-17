package ru.nsu.klochikhina.manager.service

import constants.Constants
import constants.Constants.CHUNK_SIZE
import dto.TaskDto
import enums.RequestStatus
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.data.mongodb.core.updateFirst
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.repository.RequestRepository
import java.util.UUID
import kotlin.math.min

@Service
class TaskSplitter(
    private val taskProducer: TaskProducer,
    private val requestRepository: RequestRepository,
    private val mongoTemplate: MongoTemplate
) {
    private val logger = LoggerFactory.getLogger(TaskSplitter::class.java)

    private var maxChunksPerTick: Int = 100

    @Scheduled(fixedDelayString = "\${app.taskSplitter.frequency:10000}")
    fun checkForPendingRequests() {
        val pending = requestRepository.findAllByStatus(RequestStatus.PENDING)
        for (req in pending) {
            try {
                processRequest(req.id ?: continue, req.maxLength)
            } catch (e: Exception) {
                logger.error("Error processing request ${req.id}: ${e.message}", e)
            }
        }
    }

    private fun processRequest(requestId: String, maxLength: Int) {
        val total = totalCombinations(maxLength)
        var processedThisTick = 0

        while (processedThisTick < maxChunksPerTick) {
            val query = Query(
                Criteria.where("_id").`is`(requestId)
                    .and("lastProcessedIndex").lt(total)
            )
            val update = Update().inc("lastProcessedIndex", CHUNK_SIZE)
            val options = FindAndModifyOptions.options().returnNew(false)
            val prev: HashRequest = mongoTemplate.findAndModify<HashRequest>(query, update, options) ?: break

            val start = prev.lastProcessedIndex
            if (start >= total) break

            val count = min(CHUNK_SIZE, total - start)

            val taskDto = TaskDto(
                taskId = UUID.randomUUID().toString(),
                requestId = requestId,
                startIndex = start,
                count = count,
                targetHash = prev.hash,
                maxLength = maxLength
            )

            try {
                taskProducer.persistTask(taskDto)
                mongoTemplate.updateFirst<HashRequest>(
                    Query(Criteria.where("_id").`is`(requestId)),
                    Update().inc("totalTasks", 1L)
                )
            } catch (_: DuplicateKeyException) {
                logger.warn("Duplicate task for request=$requestId start=$start — skipping (already exists)")
            } catch (e: Exception) {
                logger.error("Failed to persist task (request=$requestId start=$start): ${e.message}", e)
            }

            processedThisTick++
        }

        requestRepository.findById(requestId).ifPresent { r ->
            if (r.lastProcessedIndex >= total) {
                requestRepository.save(r.copy(status = RequestStatus.IN_PROGRESS))
                logger.info("Request $requestId: splitting finished, totalTasks=${r.totalTasks}")
            } else {
                logger.info("Request $requestId: splitting paused (lastProcessed=${r.lastProcessedIndex}, total=$total)")
            }
        }
    }

    private fun totalCombinations(maxLength: Int): Long {
        val base = Constants.BASE
        var total = 0L
        var pow = 1L
        for (len in 1..maxLength) {
            pow *= base
            if (Long.MAX_VALUE - total < pow) {
                return Long.MAX_VALUE
            }
            total += pow
        }
        return total
    }
}
