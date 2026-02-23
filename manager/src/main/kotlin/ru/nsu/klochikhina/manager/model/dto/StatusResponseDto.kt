package ru.nsu.klochikhina.manager.model.dto

import ru.nsu.klochikhina.manager.enums.RequestStatus

data class StatusResponseDto(
    val requestId: String,
    val status: RequestStatus,
    val result: String? = null
)
