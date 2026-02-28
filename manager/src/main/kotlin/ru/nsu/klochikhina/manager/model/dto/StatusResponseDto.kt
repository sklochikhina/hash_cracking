package ru.nsu.klochikhina.manager.model.dto

import enums.RequestStatus

data class StatusResponseDto(
    val requestId: String,
    val status: RequestStatus,
    // TODO("Обязательно делать, чтобы тут возвращался массив?")
    val result: String? = null
)
