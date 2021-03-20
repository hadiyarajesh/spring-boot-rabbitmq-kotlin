package com.rajesh.rabbitmq

import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.QueueBuilder
import com.rabbitmq.client.Channel
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.annotation.EnableRabbit
import org.springframework.context.annotation.Bean
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.context.annotation.Configuration
import java.lang.Exception
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder
import org.springframework.retry.interceptor.RetryOperationsInterceptor
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory

@EnableRabbit
@Configuration
class RabbitConfiguration {

    private val logger: Logger = LoggerFactory.getLogger(RabbitConfiguration::class.java)

    @Bean
    fun nonBlockingQueue(): Queue {
        return QueueBuilder.nonDurable("non-blocking-queue")
            .build()
    }

    @Bean
    fun connectionFactory(): ConnectionFactory {
        return CachingConnectionFactory("localhost")
    }

    @Bean
    fun rabbitTemplate(): RabbitTemplate {
        return RabbitTemplate(connectionFactory())
    }

    @Bean
    fun retryWaitEndedQueue(): Queue {
        return QueueBuilder.nonDurable("retry-wait-ended-queue").build()
    }

    @Bean
    fun retryQueue1(): Queue {
        return QueueBuilder.nonDurable("retry-queue-1")
            .deadLetterExchange("")
            .deadLetterRoutingKey("retry-wait-ended-queue")
            .build()
    }

    @Bean
    fun retryQueue2(): Queue {
        return QueueBuilder.nonDurable("retry-queue-2")
            .deadLetterExchange("")
            .deadLetterRoutingKey("retry-wait-ended-queue")
            .build()
    }

    @Bean
    fun retryQueue3(): Queue {
        return QueueBuilder.nonDurable("retry-queue-3")
            .deadLetterExchange("")
            .deadLetterRoutingKey("retry-wait-ended-queue")
            .build()
    }

    @Bean
    fun retryQueues(): RetryQueues {
        return RetryQueues(1000, 3.0, 10000, listOf(retryQueue1(), retryQueue2(), retryQueue3()))
    }

    @Bean
    fun observableRecoverer(): ObservableRejectAndDontRequeueRecoverer? {
        return ObservableRejectAndDontRequeueRecoverer()
    }

    @Bean
    fun retryInterceptor(): RetryOperationsInterceptor? {
        return RetryInterceptorBuilder.stateless()
            .backOffOptions(1000, 3.0, 10000)
            .maxAttempts(5)
            .recoverer(observableRecoverer())
            .build()
    }

    @Bean
    fun retryQueuesInterceptor(rabbitTemplate: RabbitTemplate?, retryQueues: RetryQueues?): RetryQueuesInterceptor? {
        return RetryQueuesInterceptor(rabbitTemplate!!, retryQueues!!)
    }

    @Bean
    fun defaultContainerFactory(connectionFactory: ConnectionFactory?): SimpleRabbitListenerContainerFactory? {
        val factory = SimpleRabbitListenerContainerFactory()
        factory.setConnectionFactory(connectionFactory)
        return factory
    }

    @Bean
    fun retryContainerFactory(
        connectionFactory: ConnectionFactory?,
        retryInterceptor: RetryOperationsInterceptor
    ): SimpleRabbitListenerContainerFactory? {
        val factory = SimpleRabbitListenerContainerFactory()
        factory.setConnectionFactory(connectionFactory)
        factory.setAdviceChain(retryInterceptor)
        return factory
    }

    @Bean
    fun retryQueuesContainerFactory(
        connectionFactory: ConnectionFactory?,
        retryInterceptor: RetryQueuesInterceptor
    ): SimpleRabbitListenerContainerFactory? {
        val factory = SimpleRabbitListenerContainerFactory()
        factory.setConnectionFactory(connectionFactory)
        factory.setAdviceChain(retryInterceptor)
        return factory
    }

    @RabbitListener(
        queues = ["non-blocking-queue"],
        containerFactory = "retryQueuesContainerFactory",
        ackMode = "MANUAL"
    )
    @Throws(Exception::class)
    fun consumeNonBlocking(payload: String?) {
        logger.info("Processing message from blocking-queue: {}", payload);
        throw Exception("Error occurred!")
    }

    @RabbitListener(queues = ["retry-wait-ended-queue"], containerFactory = "defaultContainerFactory")
    @Throws(Exception::class)
    fun consumeRetryWaitEndedMessage(payload: String, message: Message, channel: Channel) {
        val props: MessageProperties = message.messageProperties
        rabbitTemplate().convertAndSend(
            props.getHeader("x-original-exchange"),
            props.getHeader("x-original-routing-key"),
            message
        )
    }
}