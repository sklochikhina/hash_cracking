package ru.nsu.klochikhina.manager.model.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive

data class CreateRequestDto(
    @NotBlank(message = "Хэш не может быть пустым")
    val hash: String,

    @Positive(message = "Длина не может быть меньше единицы")
    val maxLength: Int
)
