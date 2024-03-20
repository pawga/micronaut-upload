package com.pawga.controllers

import io.micronaut.core.async.annotation.SingleResult
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Part
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import io.micronaut.http.multipart.CompletedPart
import io.micronaut.http.multipart.PartData
import io.micronaut.http.multipart.StreamingFileUpload
import io.micronaut.http.server.multipart.MultipartBody
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.reactivex.Flowable
import io.reactivex.Single
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.Exceptions
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import reactor.core.publisher.ReplayProcessor
import reactor.core.scheduler.Schedulers
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.security.Principal
import java.util.concurrent.atomic.LongAdder
import java.util.function.BiConsumer
import java.util.function.Function
import java.util.function.Supplier

@Controller("/upload")
class UploadController {

    @Post(consumes = [MediaType.MULTIPART_FORM_DATA])
    fun upload(file: Publisher<StreamingFileUpload>): Single<List<String>> {
        return Flowable.fromPublisher(file)
            .flatMapSingle { part: StreamingFileUpload ->
                log.debug(
                    "Uploading {} ({})",
                    part.filename,
                    part.contentType.orElse(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                )
                val tempFile = File.createTempFile("micronaut-", ".upload")
                Single.fromPublisher(part.transferTo(tempFile))
                    .map { success: Boolean? ->
                        if (!success!!) {
                            tempFile.delete()
                            throw RuntimeException("Could not write content to " + tempFile.absolutePath)
                        }
                        tempFile.absolutePath
                    }
            }
            .toList()
    }

    @Post(value = "/bytes", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN]) // (1)
    fun uploadBytes(file: ByteArray, fileName: String): HttpResponse<String> { // (2)
        return try {
            val tempFile = File.createTempFile(fileName, "temp")
            val path = Paths.get(tempFile.absolutePath)
            Files.write(path, file) // (3)
            HttpResponse.ok("Uploaded")
        } catch (e: IOException) {
            HttpResponse.badRequest("Upload Failed")
        }
    }

    @Post(value = "/whole-body", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN]) // (1)
    fun uploadBytes(@Body body: MultipartBody): Mono<String> { // (2)
        return Mono.create { emitter ->
            body.subscribe(object : Subscriber<CompletedPart> {
                private var s: Subscription? = null

                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(20)
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

    @Post(value = "/completed", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN]) // (1)
    fun uploadCompleted(file: CompletedFileUpload): HttpResponse<String> { // (2)
        return try {
            val tempFile = File.createTempFile(file.filename, "temp") //(3)
            val path = Paths.get(tempFile.absolutePath)
            Files.write(path, file.bytes) //(3)
            HttpResponse.ok("Uploaded")
        } catch (e: IOException) {
            HttpResponse.badRequest("Upload Failed")
        }
    }

    @Post(value = "/simply", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN]) // (1)
    fun upload(file: StreamingFileUpload): Mono<HttpResponse<String>> { // (2)

        val tempFile = File.createTempFile(file.filename, "temp")
        val uploadPublisher = file.transferTo(tempFile) // (3)

        return Mono.from(uploadPublisher)  // (4)
            .map { success ->
                if (success) {
                    HttpResponse.ok("Uploaded")
                } else {
                    HttpResponse.status<String>(HttpStatus.CONFLICT)
                        .body("Upload Failed")
                }
            }
    }

    @Post(value = "/outputStream", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN]) // (1)
    @SingleResult
    fun uploadOutputStream(file: StreamingFileUpload): Mono<HttpResponse<String>> { // (2)
        val outputStream = ByteArrayOutputStream() // (3)
        val uploadPublisher = file.transferTo(outputStream) // (4)

        return Mono.from(uploadPublisher) // (5)
            .map { success: Boolean ->
                return@map if (success) {
                    HttpResponse.ok("Uploaded")
                } else {
                    HttpResponse.status<String>(HttpStatus.CONFLICT)
                        .body("Upload Failed")
                }
            }
    }

    @Post(value = "/receive-json", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receiveJson(data: Data, title: String): String {
        return "$title: $data"
    }

    @Post(value = "/receive-plain", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receivePlain(data: String, title: String): String {
        return "$title: $data"
    }

    @Post(value = "/receive-bytes", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receiveBytes(data: ByteArray, title: String): String {
        return title + ": " + data.size
    }

    @Post(value = "/receive-multipart", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receiveMultipart(@Body data: Data): String {
        return data.toString()
    }

    @Post(value = "/receive-file-upload", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receiveFileUpload(data: StreamingFileUpload, title: String): Publisher<MutableHttpResponse<*>> {
        val size = data.size
        return Flux.from(data.transferTo("$title.json"))
            .map { success: Boolean ->
                if (success) HttpResponse.ok(
                    "Uploaded $size"
                ) else HttpResponse.status<Any>(HttpStatus.INTERNAL_SERVER_ERROR, "Something bad happened")
            }
            .onErrorReturn(
                HttpResponse.status<Any>(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Something bad happened"
                ) as MutableHttpResponse<*>
            )
    }

    @Post(
        value = "/receive-file-upload-input-stream",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @ExecuteOn(
        TaskExecutors.BLOCKING
    )
    @Throws(
        IOException::class
    )
    fun receiveFileUploadInputStream(data: StreamingFileUpload): String {
        data.asInputStream().use { stream ->
            return String(stream.readAllBytes(), StandardCharsets.UTF_8)
        }
    }

    @Post(
        value = "/receive-completed-file-upload",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    fun receiveCompletedFileUpload(data: CompletedFileUpload): String? {
        return try {
            data.filename + ": " + data.bytes.size
        } catch (e: IOException) {
            e.message
        }
    }

    @Post(
        value = "/receive-completed-file-upload-stream",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    fun receiveCompletedFileUploadStream(data: CompletedFileUpload): String? {
        try {
            val `is` = data.inputStream
            val size = 1024
            val buf = ByteArray(size)
            var total = 0
            var len: Int
            while ((`is`.read(buf, 0, size).also { len = it }) != -1) {
                total += len
            }
            `is`.close()
            `is`.close() //intentionally close the stream twice to ensure it doesn't throw an exception
            return data.filename + ": " + total
        } catch (e: IOException) {
            return e.message
        }
    }


    @Post(value = "/receive-publisher", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    @SingleResult
    fun receivePublisher(data: Publisher<ByteArray>?): Publisher<HttpResponse<*>> {
        return Flux.from(data).reduce(
            StringBuilder()
        ) { stringBuilder: StringBuilder, bytes: ByteArray ->
            val append = stringBuilder.append(String(bytes))
            println("bytes.length = " + bytes.size)
            append
        }
            .map(
                Function<StringBuilder, HttpResponse<*>> { stringBuilder: StringBuilder ->
                    println("stringBuilder.length() = " + stringBuilder.length)
                    val res = HttpResponse.ok(stringBuilder.toString())
                    println("res = $res")
                    res
                }
            )
    }

    @Post(value = "/receive-flow-parts", consumes = [MediaType.MULTIPART_FORM_DATA])
    @SingleResult
    fun receiveFlowParts(data: Publisher<PartData>?): Publisher<HttpResponse<*>> {
        return Flux.from(data).collectList().doOnSuccess { parts: List<PartData> ->
            for (part in parts) {
                try {
                    part.bytes //intentionally releasing the parts after all data has been received
                } catch (e: IOException) {
                    e.printStackTrace()
                }
            }
        }.map { parts: List<PartData>? -> HttpResponse.ok<Any?>() }
    }

    @Post(value = "/receive-flow-data", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    fun receiveFlowData(data: Data): Publisher<HttpResponse<*>> {
        return Flux.just(HttpResponse.ok(data.toString()))
    }

    @Post(
        value = "/receive-multiple-flow-data",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun receiveMultipleFlowData(data: Publisher<Data>): Publisher<HttpResponse<*>?> {
        return Mono.create { emitter: MonoSink<HttpResponse<*>?> ->
            data.subscribe(object : Subscriber<Data> {
                private var s: Subscription? = null
                var datas: MutableList<Data> = ArrayList()
                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(data: Data) {
                    datas.add(data)
                    s!!.request(1)
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success(HttpResponse.ok(datas.toString()))
                }
            })
        }
    }

    @Post(
        value = "/receive-two-flow-parts",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    fun receiveTwoFlowParts(
        @Part("data") dataPublisher: Publisher<String>?,
        @Part("title") titlePublisher: Publisher<String>?
    ): Publisher<HttpResponse<*>> {
        return Flux.from(titlePublisher).zipWith(dataPublisher) { title: String, data: String ->
            HttpResponse.ok(
                "$title: $data"
            )
        }
    }

    @Post(value = "/receive-multiple-completed", consumes = [MediaType.MULTIPART_FORM_DATA])
    fun receiveMultipleCompleted(
        data: Publisher<CompletedFileUpload>?,
        title: String
    ): Publisher<HttpResponse<*>> {
        val results: MutableList<Map<*, *>> = ArrayList()

        val subject = ReplayProcessor.create<HttpResponse<*>>()
        Flux.from<CompletedFileUpload>(data).subscribeOn(Schedulers.boundedElastic())
            .subscribe(object : Subscriber<CompletedFileUpload> {
                var subscription: Subscription? = null
                override fun onSubscribe(s: Subscription) {
                    s.request(1)
                    this.subscription = s
                }

                override fun onNext(upload: CompletedFileUpload) {
                    val result: MutableMap<String, Any> = LinkedHashMap()
                    result["name"] = upload.filename
                    result["size"] = upload.size
                    results.add(result)
                    subscription!!.request(1)
                }

                override fun onError(t: Throwable) {
                    subject.onError(t)
                }

                override fun onComplete() {
                    val body: MutableMap<String, Any> = LinkedHashMap()
                    body["files"] = results
                    body["title"] = title
                    subject.onNext(HttpResponse.ok<Map<String, Any>>(body))
                    subject.onComplete()
                }
            })
        return subject.asFlux()
    }

    @Post(
        value = "/receive-multiple-streaming",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun receiveMultipleStreaming(
        data: Publisher<StreamingFileUpload>?
    ): Publisher<HttpResponse<*>> {
        return Flux.from<StreamingFileUpload>(data).subscribeOn(Schedulers.boundedElastic())
            .flatMap<ByteArray> { upload: StreamingFileUpload? ->
                Flux.from<PartData>(upload)
                    .map<ByteArray> { pd: PartData ->
                        try {
                            return@map pd.bytes
                        } catch (e: IOException) {
                            throw Exceptions.propagate(e)
                        }
                    }
            }
            .collect<LongAdder>(
                Supplier<LongAdder> { LongAdder() },
                BiConsumer<LongAdder, ByteArray> { adder: LongAdder, bytes: ByteArray -> adder.add(bytes.size.toLong()) })
            .map<HttpResponse<*>> { adder: LongAdder -> HttpResponse.ok<Long>(adder.toLong()) }
    }

    @Post(value = "/receive-partdata", consumes = [MediaType.MULTIPART_FORM_DATA], produces = [MediaType.TEXT_PLAIN])
    @SingleResult
    fun receivePartdata(
        data: Publisher<PartData>?
    ): Publisher<HttpResponse<*>> {
        return Flux.from<PartData>(data).subscribeOn(Schedulers.boundedElastic())
            .map<ByteArray> { pd: PartData ->
                try {
                    val bytes = pd.bytes
                    println("received " + bytes.size + " bytes")
                    return@map bytes
                } catch (e: IOException) {
                    println("caught exception")
                    println(e)
                    throw Exceptions.propagate(e)
                }
            }
            .collect<LongAdder>(
                Supplier<LongAdder> { LongAdder() },
                BiConsumer<LongAdder, ByteArray> { adder: LongAdder, bytes: ByteArray -> adder.add(bytes.size.toLong()) })
            .map<HttpResponse<*>> { adder: LongAdder -> HttpResponse.ok<Long>(adder.toLong()) }
    }

    @Post(
        value = "/receive-multiple-publishers",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun receiveMultiplePublishers(data: Publisher<Publisher<ByteArray>>?): Publisher<HttpResponse<*>> {
        return Flux.from<Publisher<ByteArray>>(data)
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap<ByteArray> { upload: Publisher<ByteArray>? ->
                Flux.from<ByteArray>(upload).map<ByteArray> { bytes: ByteArray -> bytes }
            }
            .collect<LongAdder>(
                Supplier<LongAdder> { LongAdder() },
                BiConsumer<LongAdder, ByteArray> { adder: LongAdder, bytes: ByteArray -> adder.add(bytes.size.toLong()) })
            .map<HttpResponse<*>> { adder: LongAdder -> HttpResponse.ok<Long>(adder.toLong()) }
    }

    @Post(
        value = "/receive-flow-control",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun go(json: Map<*, *>?, file: Publisher<ByteArray>): Publisher<String?> {
        return Mono.create { singleEmitter: MonoSink<String?> ->
            file.subscribe(object : Subscriber<ByteArray> {
                private var subscription: Subscription? = null
                private val longAdder = LongAdder()
                override fun onSubscribe(subscription: Subscription) {
                    this.subscription = subscription
                    subscription.request(1)
                }

                override fun onNext(bytes: ByteArray) {
                    longAdder.add(bytes.size.toLong())
                    subscription!!.request(1)
                }

                override fun onError(throwable: Throwable) {
                    singleEmitter.error(throwable)
                }

                override fun onComplete() {
                    singleEmitter.success(longAdder.toLong().toString())
                }
            })
        }
    }

    @Post(
        value = "/receive-big-attribute",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun receiveBigAttribute(data: Publisher<PartData>): Publisher<HttpResponse<*>?> {
        return Mono.create { emitter: MonoSink<HttpResponse<*>?> ->
            data.subscribe(object : Subscriber<PartData> {
                private var s: Subscription? = null
                var datas: MutableList<String> = ArrayList()
                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(data: PartData) {
                    try {
                        datas.add(String(data.bytes, StandardCharsets.UTF_8))
                        s!!.request(1)
                    } catch (e: IOException) {
                        s!!.cancel()
                        emitter.error(e)
                    }
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success(HttpResponse.ok(java.lang.String.join("", datas)))
                }
            })
        }
    }

    @Post(
        value = "/receive-multipart-body",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun go(@Body multipartBody: MultipartBody): Publisher<String?> {
        return Mono.create { emitter: MonoSink<String?> ->
            multipartBody.subscribe(object : Subscriber<CompletedPart> {
                private var s: Subscription? = null
                var datas: MutableList<String> = ArrayList()
                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(data: CompletedPart) {
                    try {
                        datas.add(String(data.bytes, StandardCharsets.UTF_8))
                        s!!.request(1)
                    } catch (e: IOException) {
                        s!!.cancel()
                        emitter.error(e)
                    }
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success(java.lang.String.join("|", datas))
                }
            })
        }
    }

    @Post(
        value = "/receive-multipart-body-principal",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun multipartBodyWithPrincipal(principal: Principal?, @Body multipartBody: MultipartBody): Publisher<String?> {
        return Mono.create { emitter: MonoSink<String?> ->
            multipartBody.subscribe(object : Subscriber<CompletedPart> {
                private var s: Subscription? = null
                var datas: MutableList<String> = ArrayList()
                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(data: CompletedPart) {
                    try {
                        datas.add(String(data.bytes, StandardCharsets.UTF_8))
                        s!!.request(1)
                    } catch (e: IOException) {
                        s!!.cancel()
                        emitter.error(e)
                    }
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success(java.lang.String.join("|", datas))
                }
            })
        }
    }

    @Post(
        value = "/publisher-completedpart",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun publisherCompletedPart(recipients: Publisher<CompletedPart>): Publisher<String?> {
        return Mono.create { emitter: MonoSink<String> ->
            recipients.subscribe(object : Subscriber<CompletedPart> {
                private var s: Subscription? = null
                var datas: MutableList<String> = ArrayList()
                override fun onSubscribe(s: Subscription) {
                    this.s = s
                    s.request(1)
                }

                override fun onNext(data: CompletedPart) {
                    try {
                        datas.add(String(data.bytes, StandardCharsets.UTF_8))
                        s!!.request(1)
                    } catch (e: IOException) {
                        s!!.cancel()
                        emitter.error(e)
                    }
                }

                override fun onError(t: Throwable) {
                    emitter.error(t)
                }

                override fun onComplete() {
                    emitter.success(java.lang.String.join("|", datas))
                }
            })
        }
    }

    @Post(
        uri = "/receive-multipart-body-as-mono",
        consumes = [MediaType.MULTIPART_FORM_DATA],
        produces = [MediaType.TEXT_PLAIN]
    )
    @SingleResult
    fun multipartAsSingle(@Body body: MultipartBody?): Publisher<String> {
        //This will throw an exception because it caches the first result and does not emit it until
        //the publisher completes. By this time the data has been freed. The data is freed immediately
        //after the onNext call to prevent memory leaks
        return Mono.from(body).map { single: CompletedPart ->
            try {
                return@map if (single.bytes == null) "FAIL" else "OK"
            } catch (e: IOException) {
                e.printStackTrace()
            }
            "FAIL"
        }
    }

    class Data {
        var title: String? = null

        override fun toString(): String {
            return "Data{" +
                    "title='" + title + '\'' +
                    '}'
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(UploadController::class.java)
    }
}
