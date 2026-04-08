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

    //private var maxChunksPerTick: Int = 100

    @Scheduled(fixedDelayString = "\${app.taskSplitter.frequency:5000}")
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
        val request = requestRepository.findById(requestId).orElse(null) ?: return

        val totalCombinations: Long
        val totalTasks: Long
        if (request.totalCombinations == 0L) {
            val total = totalCombinations(maxLength)
            val tasks = (total + CHUNK_SIZE - 1) / CHUNK_SIZE

            val query = Query(
                Criteria.where("_id").`is`(requestId)
                    .and("totalCombinations").`is`(0L)
            )
            val update = Update()
                .set("totalCombinations", total)
                .set("totalTasks", tasks)
            val options = FindAndModifyOptions.options().returnNew(true)
            val updated = mongoTemplate.findAndModify<HashRequest>(query, update, options)

            if (updated != null) {
                totalCombinations = updated.totalCombinations
                totalTasks = updated.totalTasks
            } else {
                val refreshed = requestRepository.findById(requestId).orElseThrow()
                totalCombinations = refreshed.totalCombinations
                totalTasks = refreshed.totalTasks
            }
        } else {
            totalCombinations = request.totalCombinations
            totalTasks = request.totalTasks
        }

        var processedThisTick = 0

        while (true) {
            val query = Query(
                Criteria.where("_id").`is`(requestId)
                    .and("lastProcessedIndex").lt(totalCombinations)
            )
            val update = Update().inc("lastProcessedIndex", CHUNK_SIZE)
            val options = FindAndModifyOptions.options().returnNew(false)
            val prev: HashRequest = mongoTemplate.findAndModify<HashRequest>(query, update, options) ?: break

            val start = prev.lastProcessedIndex
            if (start >= totalCombinations) break

            val count = min(CHUNK_SIZE, totalCombinations - start)

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
            } catch (_: DuplicateKeyException) {
                logger.warn("Duplicate task for request=$requestId start=$start – skipping (already exists)")
            } catch (e: Exception) {
                logger.error("Failed to persist task (request=$requestId start=$start): ${e.message}", e)
            }

            processedThisTick++
        }

        requestRepository.findById(requestId).ifPresent { r ->
            if (r.lastProcessedIndex >= totalCombinations) {
                requestRepository.save(r.copy(status = RequestStatus.IN_PROGRESS))
                logger.info("Request $requestId: splitting finished, totalTasks=$totalTasks")
            } else {
                logger.info("Request $requestId: splitting paused (lastProcessed=${r.lastProcessedIndex}, total=$totalCombinations)")
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
