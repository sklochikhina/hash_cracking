package ru.nsu.klochikhina.manager.rabbit

import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.DirectExchange
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import rabbit.RabbitConstants

@Configuration
class RabbitConfig {

    @Bean fun taskExchange(): DirectExchange = DirectExchange(RabbitConstants.TASK_EXCHANGE, true, false)
    @Bean fun resultExchange(): DirectExchange = DirectExchange(RabbitConstants.RESULT_EXCHANGE, true, false)

    @Bean fun taskQueue(): Queue = Queue(RabbitConstants.TASK_QUEUE, true)
    @Bean fun resultQueue(): Queue = Queue(RabbitConstants.RESULT_QUEUE, true)

    @Bean fun taskBinding(taskQueue: Queue, taskExchange: DirectExchange): Binding =
        BindingBuilder.bind(taskQueue).to(taskExchange).with(RabbitConstants.TASK_ROUTING_KEY)

    @Bean fun resultBinding(resultQueue: Queue, resultExchange: DirectExchange): Binding =
        BindingBuilder.bind(resultQueue).to(resultExchange).with(RabbitConstants.RESULT_ROUTING_KEY)

    @Bean fun rabbitListenerContainerFactory(connectionFactory: ConnectionFactory): SimpleRabbitListenerContainerFactory {
        val factory = SimpleRabbitListenerContainerFactory()
        factory.setConnectionFactory(connectionFactory)
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL)
        return factory
    }
}