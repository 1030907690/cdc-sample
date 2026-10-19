package com.zzq.controller;


import com.zzq.request.UserRequest;
import com.zzq.response.UserResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 1/18/2026 5:18 PM
 */
@Tag(name = "用户")
@RestController
@RequestMapping("/api/user")
public class UserController {


    @GetMapping("/list")
    @Operation(summary = "用户列表")
    public List<UserResponse> list(UserRequest request) {
        return new ArrayList<>();
    }

    @GetMapping("/{id}")
    @Operation(summary = "用户详情")
    public UserResponse detail(@Parameter(required = true, description = "用户ID") @PathVariable("id") Integer id) {
        return new UserResponse();
    }
}
