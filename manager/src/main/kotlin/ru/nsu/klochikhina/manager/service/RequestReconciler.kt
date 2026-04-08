package ru.nsu.klochikhina.manager.service

import enums.RequestStatus
import enums.TaskStatus
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.count
import org.springframework.data.mongodb.core.find
import org.springframework.data.mongodb.core.findAndModify
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import ru.nsu.klochikhina.manager.model.entity.HashRequest
import ru.nsu.klochikhina.manager.model.entity.Task

@Service
class RequestReconciler(
    private val mongoTemplate: MongoTemplate
) {
    private val logger = LoggerFactory.getLogger(RequestReconciler::class.java)

    @Scheduled(fixedDelayString = "\${app.reconcile.interval:2000}")
    fun reconcileRequests() {
        val requests = mongoTemplate.find<HashRequest>(
            Query(Criteria.where("status").`is`(RequestStatus.IN_PROGRESS))
        )

        for (request in requests) {
            reconcileOne(request)
        }
    }

    private fun reconcileOne(request: HashRequest) {
        val requestId = request.id

        val doneCount = mongoTemplate.count<Task>(
            Query(
                Criteria.where("requestId").`is`(requestId)
                    .and("status").`is`(TaskStatus.DONE)
            )
        )

        val results = mongoTemplate.find<Task>(
            Query(
                Criteria.where("requestId").`is`(requestId)
                    .and("status").`is`(TaskStatus.DONE)
                    .and("resultset.0").exists(true)
            )
        ).flatMap { it.resultset }.distinct()

        if (results.isNotEmpty()) {
            val query = Query(
                Criteria.where("_id").`is`(requestId)
                    .and("status").`is`(RequestStatus.IN_PROGRESS)
            )

            val update = Update()
                .set("status", RequestStatus.READY)
                .addToSet("results").each(*results.toTypedArray())

            val updated = mongoTemplate.findAndModify<HashRequest>(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true)
            )

            if (updated != null) {
                logger.info("Request $requestId reconciled to READY with ${results.size} results")
            }
            return
        }

        if (doneCount >= request.totalTasks) {
            val query = Query(
                Criteria.where("_id").`is`(requestId)
                    .and("status").`is`(RequestStatus.IN_PROGRESS)
            )

            val update = Update().set("status", RequestStatus.ERROR)

            val updated = mongoTemplate.findAndModify<HashRequest>(
                query,
                update,
                FindAndModifyOptions.options().returnNew(true)
            )

            if (updated != null) {
                logger.info("Request $requestId reconciled to ERROR (all tasks done, no results)")
            }
        }
    }
}
