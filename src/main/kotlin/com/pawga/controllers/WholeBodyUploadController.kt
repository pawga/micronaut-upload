package com.pawga.controllers

import io.micronaut.http.MediaType.MULTIPART_FORM_DATA
import io.micronaut.http.MediaType.TEXT_PLAIN
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import io.micronaut.http.multipart.CompletedPart
import io.micronaut.http.server.multipart.MultipartBody
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.publisher.Mono

/**
 * Created by sivannikov on 18.03.2024 12:26
 */
@Controller("/upload")
class WholeBodyUploadController {

    @Post(value = "/whole-body", consumes = [MULTIPART_FORM_DATA], produces = [TEXT_PLAIN]) // (1)
    fun uploadBytes(@Body body: MultipartBody): Mono<String> { // (2)
        return Mono.create { emitter ->
            body.subscribe(object : Subscriber<CompletedPart> {
                private var s: Subscription? = null

                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(completedPart: CompletedPart) {
                    val partName = completedPart.name
                    if (completedPart is CompletedFileUpload) {
                        val originalFileName = completedPart.filename
                    }
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success("Uploaded")
                }
            })
        }
    }
}