package ru.nsu.klochikhina.manager.repository

import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository
import ru.nsu.klochikhina.manager.model.entity.QueuedTask

@Repository
interface QueuedTaskRepository : MongoRepository<QueuedTask, String>