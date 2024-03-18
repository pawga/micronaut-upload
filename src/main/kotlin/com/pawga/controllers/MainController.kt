package com.pawga.controllers

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get

@Controller("/upload")
class MainController {

    @Get(uri="/", produces=["text/plain"])
    fun index(): String {
        return "Example Response"
    }
}