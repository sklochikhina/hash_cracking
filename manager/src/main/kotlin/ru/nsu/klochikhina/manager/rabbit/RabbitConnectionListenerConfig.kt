package ru.nsu.klochikhina.manager.rabbit

import jakarta.annotation.PostConstruct
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.Connection
import org.springframework.amqp.rabbit.connection.ConnectionListener
import org.springframework.context.annotation.Configuration
import ru.nsu.klochikhina.manager.service.QueueRetryService

@Configuration
class RabbitConnectionListenerConfig(
    private val connectionFactory: CachingConnectionFactory,
    private val queueRetryService: QueueRetryService
) {
    private val logger = org.slf4j.LoggerFactory.getLogger(RabbitConnectionListenerConfig::class.java)

    @PostConstruct
    fun init() {
        connectionFactory.addConnectionListener(object : ConnectionListener {
            override fun onCreate(connection: Connection?) {
                logger.info("Rabbit connection created — triggering queued tasks processing")
                queueRetryService.processQueuedNow()
            }

            override fun onClose(connection: Connection) {}
        })
    }
}