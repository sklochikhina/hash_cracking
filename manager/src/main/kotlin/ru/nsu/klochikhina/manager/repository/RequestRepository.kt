package ru.nsu.klochikhina.manager.repository

import enums.RequestStatus
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import ru.nsu.klochikhina.manager.model.entity.HashRequest

@Repository
interface RequestRepository : MongoRepository<HashRequest, String> {
    fun findAllByStatus(status: RequestStatus): MutableList<HashRequest>
    fun findFirstByHashAndMaxLength(hash: String, maxLength: Int): HashRequest?
}
