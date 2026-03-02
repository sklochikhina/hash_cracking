package ru.nsu.klochikhina.manager.service

import constants.Constants
import constants.Constants.CHUNK_SIZE
import dto.TaskDto
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.repository.RequestRepository
import java.util.UUID

@Service
class TaskSplitter(
    private val taskProducer: TaskProducer,
    private val requestRepository: RequestRepository
) {

    private val logger = org.slf4j.LoggerFactory.getLogger(TaskSplitter::class.java)

    @Async("taskExecutor")
    fun splitAndSendAsync(requestId: String, targetHash: String, maxLength: Int) {
        logger.info("splitAndSendAsync start request=$requestId")
        val total = totalCombinations(maxLength)
        var start = 0L
        var tasksCount = 0L

        while (start < total) {
            val count = minOf(CHUNK_SIZE, total - start)
            val task = TaskDto(
                taskId = UUID.randomUUID().toString(),
                requestId = requestId,
                startIndex = start,
                count = count,
                targetHash = targetHash,
                maxLength = maxLength
            )
            taskProducer.sendOrQueueTask(task)
            tasksCount++
            start += count
        }

        val opt = requestRepository.findById(requestId)
        if (opt.isPresent) {
            val r = opt.get()
            requestRepository.save(r.copy(totalTasks = tasksCount))
            logger.info("splitAndSendAsync finished request=$requestId totalTasks=$tasksCount")
        } else {
            logger.warn("splitAndSendAsync: request not found $requestId")
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