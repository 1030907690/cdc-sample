# cdc-sample

> 基于 **Spring Boot 3 + Flink CDC** 的 MySQL 实时数据变更捕获（Change Data Capture）示例项目。
> 在 Spring Boot 应用启动后自动拉起一个 Flink CDC 作业，监听 MySQL 指定表的 `INSERT / UPDATE / DELETE` 变更并分发处理。

---

## 📌 Release v1.0.0 更新内容

首个正式版本，提供一条可运行的 Flink CDC 示例链路，主要功能如下：

### 新增功能
- **MySQL CDC 监听**：基于 Flink CDC 3.5.0（Flink 1.20.3）`MySqlSource` DataStream API，监听 MySQL `jjjy181` 库的 `eb_product` 表。
- **全量 + 增量一体化**：采用 `StartupOptions.initial()`，先对存量数据做全量快照（`op=r`），再无缝切换为 binlog 增量监听（`op=c/u/d`）。
- **变更事件分发**：将 Debezium JSON 事件按 `op` 字段解析并分发到 `handleInsert / handleUpdate / handleDelete` 三个处理器（当前为日志占位，业务可在此填充）。
- **Spring Boot 无缝集成**：通过 `ApplicationReadyEvent` 在应用完全就绪后，于独立线程启动 Flink CDC 作业，避免阻塞 Tomcat 主线程。
- **作业可靠性配置**：开启 60s `EXACTLY_ONCE` Checkpoint + 固定延迟重启策略（失败重试 3 次，间隔 2s），支持断点续传。
- **OpenAPI 接口文档**：集成 springdoc-openapi 3 + Knife4j 4.5 增强 UI，提供 `index` / `user` 两组 API 文档分组，含示例接口。

### 已知限制 / TODO
- `handleInsert / handleUpdate / handleDelete` 目前仅打印日志，业务处理逻辑待实现。
- MySQL 连接信息（`host / port / user / password / database`）硬编码在 `FlinkChangeDataCapture` 中，后续应抽到配置文件。
- 监听范围目前仅限 `eb_product` 单表（规避无主键表 chunk key 报错），多表/多库监听待扩展。

---

## 技术栈

| 组件 | 版本 |
| --- | --- |
| JDK | 17 |
| Spring Boot | 3.4.4 |
| Apache Flink | 1.20.3 |
| Flink CDC (MySQL Connector) | 3.5.0 |
| springdoc-openapi | 2.8.13 |
| Knife4j | 4.5.0 |
| MySQL Connector/J | 由 Spring Boot BOM 统一管理 |

---

## 快速开始

### 环境准备
- JDK 17+
- Maven 3.8+
- 一台可访问的 MySQL 实例（需开启 binlog，格式为 `ROW`）

### 配置数据库（binlog 前置要求）
```sql
-- 确认 binlog 已开启且为 ROW 模式
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
```

### 修改连接配置
编辑 `src/main/java/com/zzq/cdc/FlinkChangeDataCapture.java` 顶部的常量：
```java
private static final String HOST     = "192.168.10.131";
private static final int    PORT     = 3306;
private static final String USERNAME = "root";
private static final String PASSWORD = "root";
private static final String DATABASE = "jjjy181";
// 监听范围：DATABASE + ".eb_product"
```

### 运行
```bash
mvn spring-boot:run
# 或
mvn clean package && java -jar target/cdc-sample-0.0.1-SNAPSHOT.jar
```
应用启动后日志会输出 `Spring Boot 启动完成，拉起后置任务`，随后 Flink CDC 作业开始监听。

---

## 架构与原理

```
MySQL (binlog)
      │
      ▼
FlinkChangeDataCapture  ── MySqlSource (DataStream API)
      │                     · JsonDebeziumDeserializationSchema
      │                     · StartupOptions.initial() 全量+增量
      ▼
CdcEventDispatcher (ProcessFunction, static)
      │  解析 op 字段：c / u / d / r
      ├── c / r → handleInsert()   (新增 / 全量快照)
      ├── u     → handleUpdate()   (修改前 + 修改后)
      └── d     → handleDelete()   (删除前)
      │
      ▼
   业务处理逻辑（TODO：在此填充）
```

**关键设计**
- **算子可序列化**：`CdcEventDispatcher` 设计为 `static` 内部类，避免隐式持有外部类引用导致 Flink 序列化失败。
- **非阻塞启动**：`env.execute()` 为阻塞调用，因此放到 `flink-cdc-worker` 独立线程中执行，保证 Spring Boot / Tomcat 正常启动。
- **本地执行环境**：使用 `createLocalEnvironment()` 显式指定本地模式，规避独立 Java 程序找不到 `ExecutorFactory` 的问题。

---

## 配置说明

主要配置位于 `src/main/resources/application.yml`：

| 配置项 | 说明 |
| --- | --- |
| `server.port` | Web 服务端口，当前配置为 `8080` |
| `springdoc.api-docs.path` | OpenAPI 文档路径，配置为 `/v3/api-docs` |
| `springdoc.swagger-ui.path` | Swagger UI 路径，配置为 `/swagger-ui.html` |
| `knife4j.enable` | Knife4j 增强开关 |
| `logging.level.com.zzq` | `com.zzq` 包日志级别，配置为 `debug` |

Flink Web UI 端口在代码中设为 `8081`（`RestOptions.PORT`）。

---

## API 文档

启动后访问：
- Swagger UI：`http://localhost:8080/swagger-ui.html`
- Knife4j 增强文档：`http://localhost:8080/doc.html`（增强开启时）
- OpenAPI JSON：`http://localhost:8080/v3/api-docs`

内置示例接口：
| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/index/` | 首页接口 |
| GET | `/api/user/list` | 用户列表（占位） |
| GET | `/api/user/{id}` | 用户详情（占位） |

---

## 目录结构

```
src/main/java/com/zzq/
├── CDCSampleApplication.java          # Spring Boot 启动类
├── cdc/
│   └── FlinkChangeDataCapture.java    # Flink CDC 核心监听与事件分发
├── config/
│   └── OpenApiConfig.java             # OpenAPI / Knife4j 配置
├── controller/
│   ├── IndexController.java           # 首页示例接口
│   └── UserController.java            # 用户示例接口
├── listener/
│   └── ApplicationRefreshedListener.java  # 启动后置任务（拉起 CDC）
├── request/
│   └── UserRequest.java               # 示例请求 DTO
├── response/
│   └── UserResponse.java              # 示例响应 DTO
└── resources/
    └── application.yml                # 应用配置
```

---

## 说明

本项目仅供学习与示例演示使用。
