package com.zzq.controller;


import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author: Zhou Zhongqing
 * @date: 1/18/2026 9:50 PM
 */
@Tag(name = "首页")
@RestController
@RequestMapping("/api/index")
public class IndexController {

    @GetMapping("/")
    @Operation(summary = "首页接口")
    public String index() {
        return "index";
    }

}
