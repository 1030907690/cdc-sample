# cdc-sample

> 基于 **Spring Boot 3** 的 MySQL 实时数据变更捕获（Change Data Capture）示例项目。
> 内置 **两套 CDC 实现**（Flink CDC 3.5 / Debezium 内嵌引擎），通过常量一键切换。
> 在 Spring Boot 应用启动后自动拉起 CDC 作业，监听 MySQL 指定表的 `INSERT / UPDATE / DELETE` 变更并分发处理。

---

## 📌 Release v1.0.1 更新内容

新增 **Debezium 内嵌引擎** 方案，与 Flink CDC 并存，通过常量一键切换。

### 新增功能
- **双方案并存 + 一键切换**：`ApplicationRefreshedListener` 顶部通过 `CDC_IMPL` 常量（`"flink"` / `"debezium"`）决定启动哪套 CDC。
- **Debezium 内嵌引擎方案**：纯 Java + Debezium 1.9.8，零 Flink 依赖，启动速度更快（~1s vs ~5s），JVM 占用更低（~60MB vs ~150MB）。
- **纯内存存储支持**：Debezium 方案使用 `MemoryOffsetBackingStore` + `MemoryDatabaseHistory`，**零磁盘依赖**，适合 k3s Pod 部署（不需要挂载 PVC）。Pod 重启后从 MySQL 最新 binlog 位点继续监听（需 MySQL `expire_logs_days` ≥ Pod 重启间隔）。
- **schema_only 启动模式**：跳过全量快照，直接从最新 binlog 位点开始监听，**不触发无主键表 chunk key 错误**。
- **诊断日志加强**：`DebeziumChangeDataCapture` 内置事件计数器 + 启动后 5s 自检日志，便于确认引擎是否正常运行。
- **监听范围精确配置**：两套方案都已限定监听 `jjjy181.eb_product` 单表（正则匹配）。

### 方案对比

| 对比项 | Flink CDC 3.5 | Debezium 内嵌引擎 |
|---|---|---|
| 新增依赖数 | 7 个 | 5 个（含 connect-runtime + kafka-clients，**但只用到 MemoryOffsetBackingStore**，不连 Kafka Broker） |
| 启动耗时 | ~5s（LocalExecutor 初始化） | ~1s |
| JVM 内存占用 | ~150MB | ~60MB |
| 断点续传 | ✅ Flink Checkpoint（持久化） | ⚠️ 默认内存（重启丢），可切 File 版 |
| 无主键表 | ❌ initial() 报错，需禁用 chunk | ✅ schema_only 不受影响 |
| 适合场景 | 需要窗口/状态/Join 的流处理 | 轻量监听 + 回调业务逻辑 |

### 踩过的坑（记录备忘）
- **Flink CDC 3.5**：`tableList` / `serverTimeZone` 是强制 checkNotNull；`LocalExecutor` 需要 `flink-clients`；缺 `flink-connector-base` / `flink-table-common` 两个隐藏传递依赖；匿名内部类持有外部类引用导致序列化失败（必须 static）；无主键表触发 chunk key 校验。
- **Debezium 内嵌引擎 1.9.8**：OffsetBackingStore 接口在 Kafka Connect 3.2.0 的 connect-runtime 里（1.9.8 针对 connect 3.2.0 编译，用 3.8.1 会无参构造签名不匹配）；`FileOffsetBackingStore` / `MemoryOffsetBackingStore` 都在 connect-runtime 里（connect-api 只有接口定义，没实现类）；`schema_only + FileDatabaseHistory` 组合报 "db history topic missing"（因为 history 文件不存在，Memory 版或 initial 模式才不会触发）。

### 已知限制 / TODO
- `handleInsert / handleUpdate / handleDelete` 目前仅打印日志，业务处理逻辑待实现。
- MySQL 连接信息（`host / port / user / password / database`）硬编码在两个 CDC 类中，后续应抽到配置文件。
- Flink CDC 的 `StartupOptions.initial()` 需要表有主键（否则报 chunk key 错误），无主键表请用 Debezium 方案。
- Debezium 方案重启后从最新 binlog 位点继续（丢失重启期间的变更），对数据零丢失要求高的场景请切 File 存储 + 挂载 PVC。

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
| Debezium | 1.9.8.Final（内嵌引擎方案） |
| Kafka Connect Runtime | 3.2.0（提供 MemoryOffsetBackingStore，不连 Broker） |
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
两套 CDC 方案的常量都硬编码在各自类中：

- **Flink CDC**：编辑 `src/main/java/com/zzq/cdc/FlinkChangeDataCapture.java` 顶部的常量
- **Debezium**：编辑 `src/main/java/com/zzq/cdc/DebeziumChangeDataCapture.java` 顶部的常量

```java
private static final String HOST     = "192.168.10.131";
private static final int    PORT     = 3306;
private static final String USERNAME = "root";
private static final String PASSWORD = "root";
private static final String DATABASE = "jjjy181";
```

### 切换 CDC 方案
编辑 `src/main/java/com/zzq/listener/ApplicationRefreshedListener.java` 顶部的 `CDC_IMPL` 常量：

```java
private static final String CDC_IMPL = "debezium";  // "flink" 或 "debezium"
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
                    ApplicationRefreshedListener
                    （CDC_IMPL 常量决定启动哪套）
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    FlinkChangeDataCapture          DebeziumChangeDataCapture
    （Flink CDC 3.5 DataStream）    （Debezium 内嵌引擎）
              │                               │
              │  MySqlSource                  │  EmbeddedEngine
              │  · JsonDebeziumDeserializationSchema     · MemoryOffsetBackingStore
              │  · StartupOptions.initial()              · MemoryDatabaseHistory
              │                                          · schema_only（跳过快照）
              ▼                               ▼
    CdcEventDispatcher                  DebeziumChangeDataCapture (内部回调)
    (ProcessFunction, static)           按 op 分发：
      │  解析 op 字段：c / u / d / r     · c / r → handleInsert()
      ├── c / r → handleInsert()        · u     → handleUpdate()
      ├── u     → handleUpdate()        · d     → handleDelete()
      └── d     → handleDelete()
              │                               │
              └───────────────┬───────────────┘
                              ▼
                   业务处理逻辑（TODO：在此填充）
```

**Flink CDC 关键设计**
- **算子可序列化**：`CdcEventDispatcher` 设计为 `static` 内部类，避免隐式持有外部类引用导致 Flink 序列化失败。
- **非阻塞启动**：`env.execute()` 为阻塞调用，因此放到独立线程中执行，保证 Spring Boot / Tomcat 正常启动。
- **本地执行环境**：使用 `createLocalEnvironment()` 显式指定本地模式，规避独立 Java 程序找不到 `ExecutorFactory` 的问题。

**Debezium 内嵌引擎关键设计**
- **纯内存存储**：`MemoryOffsetBackingStore` + `MemoryDatabaseHistory`，零磁盘依赖，Pod 重启从最新 binlog 位点继续。
- **schema_only 模式**：跳过全量快照，直接监听增量变更，无主键表也不受影响。
- **独立线程**：`DebeziumEngine.run()` 阻塞，放到 `debezium-cdc-worker` 独立线程执行。

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
│   ├── FlinkChangeDataCapture.java    # Flink CDC 3.5 DataStream API 实现
│   └── DebeziumChangeDataCapture.java # Debezium 内嵌引擎实现（纯 Java）
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

本项目由 **AI Seed-Code** + **DeepSeek-V4-Pro** 辅助编写。
