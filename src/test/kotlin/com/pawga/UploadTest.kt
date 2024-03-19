package com.pawga
import io.micronaut.core.io.ResourceLoader
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.client.multipart.MultipartBody
import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.netty.handler.codec.http.HttpResponseStatus
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledInNativeImage
import java.io.File
import java.util.*

@MicronautTest
class UploadTest {

    private val hexFormat: HexFormat = HexFormat.of()

    @Inject
    lateinit var application: EmbeddedApplication<*>

    @Test
    fun testItWorks() {
        Assertions.assertTrue(application.isRunning)
    }


    @DisabledInNativeImage
    @Test
    fun `returns httpResponseOK if upload was successful`(
        @Client("/") httpClient: HttpClient,  // <2>
        resourceLoader: ResourceLoader
    ) {
        //prepare
        val file = File.createTempFile("data", ".zip")
        val requestBody = MultipartBody.builder()
            .addPart(
                "file",
                file.name,
                MediaType.TEXT_PLAIN_TYPE,
                file
            ).build()
        //when
        val request = HttpRequest.POST("/upload/", requestBody)
            .contentType(MediaType.MULTIPART_FORM_DATA_TYPE)
        val response: HttpResponse<String> =
            httpClient.toBlocking().exchange<MultipartBody, String>(request, String::class.java)
        //then
        assertThat(response.status).isEqualTo(HttpStatus.OK)
        assertThat(response.contentType.get()).isEqualTo(MediaType.TEXT_PLAIN_TYPE)
        assertThat(response.body()).isEqualTo("Uploaded")
    }

    @DisabledInNativeImage
    @Test
    fun `returns httpResponse upload failed if upload was not successful`(
        @Client("/") httpClient: HttpClient,  // <2>
        resourceLoader: ResourceLoader
    ) {
        val file = File.createTempFile("data", ".txt")
        val requestBody = MultipartBody.builder()
            .addPart(
                "file",
                file.name,
                MediaType.TEXT_PLAIN_TYPE,
                file
            ).build()

        val request = HttpRequest.POST("/", requestBody)
            .contentType(MediaType.MULTIPART_FORM_DATA_TYPE)

        val exception = Assertions.assertThrows(HttpClientResponseException::class.java) {
            httpClient.toBlocking().exchange<MultipartBody, String>(request, String::class.java)
        }
        assertThat(exception.response.status).isNotEqualTo(HttpStatus.OK)
    }

}
