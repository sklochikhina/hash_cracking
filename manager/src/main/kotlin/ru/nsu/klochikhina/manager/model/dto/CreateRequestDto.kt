package ru.nsu.klochikhina.manager.model.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive

data class CreateRequestDto(
    @field:NotBlank
    val hash: String,

    @field:NotBlank
    @field:Positive
    val maxLength: Int
)
