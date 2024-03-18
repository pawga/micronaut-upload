package com.pawga.controllers

import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType.MULTIPART_FORM_DATA
import io.micronaut.http.MediaType.TEXT_PLAIN
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Created by sivannikov on 18.03.2024 12:25
 */
@Controller("/upload")
class BytesUploadController {

    @Post(value = "/bytes", consumes = [MULTIPART_FORM_DATA], produces = [TEXT_PLAIN]) // (1)
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
}