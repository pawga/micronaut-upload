package com.pawga.controllers

import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType.MULTIPART_FORM_DATA
import io.micronaut.http.MediaType.TEXT_PLAIN
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.multipart.CompletedFileUpload
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Created by sivannikov on 18.03.2024 12:21
 */
@Controller("/upload")
class CompletedUploadController {
    @Post(value = "/completed", consumes = [MULTIPART_FORM_DATA], produces = [TEXT_PLAIN]) // (1)
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
}