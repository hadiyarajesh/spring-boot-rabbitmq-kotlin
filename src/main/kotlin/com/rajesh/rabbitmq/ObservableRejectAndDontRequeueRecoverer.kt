package com.rajesh.rabbitmq

import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer

class ObservableRejectAndDontRequeueRecoverer : RejectAndDontRequeueRecoverer() {
    private lateinit var observer: Runnable

    override fun recover(message: Message?, cause: Throwable?) {
        observer.run()
        super.recover(message, cause);
    }

    fun setObserver(observer: Runnable?) {
        this.observer = observer!!
    }
}