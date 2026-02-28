package ru.nsu.klochikhina.manager.controller

import dto.TaskDto
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.nsu.klochikhina.manager.model.dto.CreateRequestDto
import ru.nsu.klochikhina.manager.model.dto.CreateRequestResponseDto
import ru.nsu.klochikhina.manager.model.dto.StatusResponseDto
import ru.nsu.klochikhina.manager.rabbit.TaskProducer
import ru.nsu.klochikhina.manager.service.HashService
import java.util.UUID

@RestController
@RequestMapping("/api/hash")
class HashController(
    private val hashService: HashService,
    private val taskProducer: TaskProducer // для тестирования
) {

    @PostMapping("/crack")
    fun crackHash(@Valid @RequestBody request: CreateRequestDto): CreateRequestResponseDto {
        val requestId = hashService.createRequest(request.hash, request.maxLength)
        return CreateRequestResponseDto(requestId)
    }

    @GetMapping("/status")
    fun getStatus(@RequestParam requestId: String): StatusResponseDto {
        val request = hashService.getStatus(requestId)
        return StatusResponseDto(
            request.id!!,
            request.status,
            request.result
        )
    }

    // для тестирования
    @PostMapping("/test/send-task")
    fun sendTestTask(): ResponseEntity<String> {
        val demo = TaskDto(
            taskId = UUID.randomUUID().toString(),
            requestId = "test-request-${UUID.randomUUID()}",
            startIndex = 0L,
            count = 1000L,
            targetHash = "e2fc714c4727ee9395f324cd2e7f331f",
            maxLength = 4
        )
        taskProducer.sendTask(demo)
        return ResponseEntity.ok("task sent: ${demo.taskId}")
    }
}