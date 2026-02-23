package ru.nsu.klochikhina.manager.controller

import jakarta.validation.Valid
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.nsu.klochikhina.manager.model.dto.CreateRequestDto
import ru.nsu.klochikhina.manager.model.dto.CreateRequestResponseDto
import ru.nsu.klochikhina.manager.model.dto.StatusResponseDto
import ru.nsu.klochikhina.manager.service.HashService

@RestController
@RequestMapping("/api/hash")
class HashController(
    private val hashService: HashService
) {

    @PostMapping("/crack")
    fun crackHash(@Valid @RequestBody request: CreateRequestDto): CreateRequestResponseDto {
        val requestId = hashService.createRequest(request.hash, request.maxLength)
        return CreateRequestResponseDto(requestId)
    }

    @GetMapping("/status")
    fun getStatus(@RequestParam requestId: String): StatusResponseDto {
        val request = hashService.getStatus(requestId)
        // TODO("Тут нужно будет поменять поле status на string, если ответ будет корявым")
        return StatusResponseDto(
            request.id!!,
            request.status,
            request.result
        )
    }
}