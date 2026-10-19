package com.zzq.request;


import io.swagger.v3.oas.annotations.media.Schema;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 1/18/2026 5:19 PM
 */
@Schema(description = "用户请求参数")
public class UserRequest {

    @Schema(description = "用户ID")
    private Integer id;

    @Schema(description = "用户名称")
    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
