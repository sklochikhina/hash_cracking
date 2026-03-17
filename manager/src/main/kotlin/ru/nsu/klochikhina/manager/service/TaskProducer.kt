package ru.nsu.klochikhina.manager.service

import dto.TaskDto
import enums.TaskStatus
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.Task
import ru.nsu.klochikhina.manager.repository.TaskRepository
import java.time.Instant
import java.util.UUID

@Service
class TaskProducer(
    private val taskRepository: TaskRepository
) {
    private val logger = LoggerFactory.getLogger(TaskProducer::class.java)

    fun persistTask(taskDto: TaskDto) {
        val t = Task(
            id = UUID.randomUUID().toString(),
            requestId = taskDto.requestId,
            startIndex = taskDto.startIndex,
            count = taskDto.count,
            targetHash = taskDto.targetHash,
            maxLength = taskDto.maxLength,
            status = TaskStatus.QUEUED,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        taskRepository.save(t)
        logger.info("persistTask saved task=${t.id} request=${t.requestId} start=${t.startIndex} count=${t.count}")
    }
}
