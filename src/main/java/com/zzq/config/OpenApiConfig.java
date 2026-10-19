package com.zzq.config;


import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @description:
 * @author: Zhou Zhongqing
 * @date: 1/18/2026 5:04 PM
 */
@Configuration
public class OpenApiConfig {


    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("zzq测试title")
                        .version("1.0.0")
                        .description("zzq测试title desc")
                        .contact(new Contact()
                                .name("zzq")
                                .email("zzq@xx.com"))
                )
                .externalDocs(new ExternalDocumentation()
                        .description("项目文档")
                        .url("https://github.com/1030907690/spring-doc-sample"));
    }
}
