package com.pawga.controllers

import io.micronaut.core.async.annotation.SingleResult
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus.CONFLICT
import io.micronaut.http.MediaType.MULTIPART_FORM_DATA
import io.micronaut.http.MediaType.TEXT_PLAIN
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.StreamingFileUpload
import reactor.core.publisher.Mono
import java.io.ByteArrayOutputStream
import java.io.File

/**
 * Created by sivannikov on 18.03.2024 12:23
 */
@Controller("/upload")
class UploadController {

    @Post(value = "/", consumes = [MULTIPART_FORM_DATA], produces = [TEXT_PLAIN]) // (1)
    fun upload(file: StreamingFileUpload): Mono<HttpResponse<String>> { // (2)

        val tempFile = File.createTempFile(file.filename, "temp")
        val uploadPublisher = file.transferTo(tempFile) // (3)

        return Mono.from(uploadPublisher)  // (4)
            .map { success ->
                if (success) {
                    HttpResponse.ok("Uploaded")
                } else {
                    HttpResponse.status<String>(CONFLICT)
                        .body("Upload Failed")
                }
            }
    }

    @Post(value = "/outputStream", consumes = [MULTIPART_FORM_DATA], produces = [TEXT_PLAIN]) // (1)
    @SingleResult
    fun uploadOutputStream(file: StreamingFileUpload): Mono<HttpResponse<String>> { // (2)
        val outputStream = ByteArrayOutputStream() // (3)
        val uploadPublisher = file.transferTo(outputStream) // (4)

        return Mono.from(uploadPublisher) // (5)
            .map { success: Boolean ->
                return@map if (success) {
                    HttpResponse.ok("Uploaded")
                } else {
                    HttpResponse.status<String>(CONFLICT)
                        .body("Upload Failed")
                }
            }
    }
}