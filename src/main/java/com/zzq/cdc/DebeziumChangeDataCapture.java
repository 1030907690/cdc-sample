package com.zzq.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Debezium 内嵌引擎 CDC 监听类：纯 Java 方式监听 MySQL jjjy181 库的增删改操作，无需 Flink。
 * <p>
 * 与 {@link FlinkChangeDataCapture}（Flink CDC 3.5 DataStream API 方案）是两套独立实现，
 * 共享同一个 MySQL 数据源，但底层引擎不同。
 * </p>
 *
 * <h3>技术选型</h3>
 * <ul>
 *   <li>{@link DebeziumEngine} 内嵌模式，直接 run() 阻塞当前线程</li>
 *   <li>{@link MySqlConnector} MySQL binlog 连接器</li>
 *   <li>JSON 格式输出（{@link Json}），用 Jackson 解析 Debezium Envelope</li>
 *   <li>{@code MemoryDatabaseHistory} 存储 offset（重启后从 binlog 最新位点重新开始）</li>
 * </ul>
 *
 * <h3>op 字段含义</h3>
 * <ul>
 *   <li>{@code c} — create（新增）</li>
 *   <li>{@code u} — update（修改）</li>
 *   <li>{@code d} — delete（删除）</li>
 *   <li>{@code r} — read（全量快照阶段的初始读取，等价于新增）</li>
 * </ul>
 */
public class DebeziumChangeDataCapture {

    private static final Logger log = LoggerFactory.getLogger(DebeziumChangeDataCapture.class);

    // ====== MySQL 连接配置常量（与 Flink CDC 方案相同） ======
    private static final String HOST     = "127.0.0.1";
    private static final int    PORT     = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String DATABASE = "jjjy181";

    // ====== Debezium 专有配置 ======
    // serverId 必须全局唯一（MySQL 实例内所有 Debezium / Flink CDC 任务不能重复）
    private static final String SERVER_ID   = "5401";
    // 任意唯一名称，用于标识这个 Debezium 实例
    private static final String SERVER_NAME = DATABASE + "-cdc-debezium";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Debezium 引擎实例，用于后续 shutdown。 */
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    /**
     * 启动 Debezium CDC 监听（阻塞方法，必须在独立线程中调用）。
     * <p>
     * {@code run()} 会阻塞当前线程持续消费 binlog 变更，直到 {@link #stop()} 被调用。
     * </p>
     *
     * @throws Exception 配置或运行异常
     */
    public void start() throws Exception {
        Properties props = buildDebeziumProperties();

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::handleChangeEvent)
                .build();

        log.info("Debezium CDC 引擎启动中，监听 MySQL {}:{} / {} ...", HOST, PORT, DATABASE);

        // 直接在当前线程阻塞运行；由外部调用方负责把本方法放进独立线程。
        engine.run();
    }

    /**
     * 优雅关闭 Debezium 引擎。
     * <p>
     * 会等待引擎处理完已收到的事件后停止，并释放 MySQL 上的 Binlog Dump 连接。
     * </p>
     */
    public void stop() {
        if (engine != null) {
            log.info("关闭 Debezium CDC 引擎...");
            try {
                engine.close();
            } catch (Exception e) {
                log.warn("关闭 Debezium 引擎时发生异常（可忽略）", e);
            }
        }
    }

    // ==================== Debezium 事件处理 ====================

    /**
     * Debezium 事件回调。收到 binlog 变更后按 op 类型分发。
     *
     * @param event Debezium 变更事件，key/value 均为 JSON 字符串
     */
    private void handleChangeEvent(ChangeEvent<String, String> event) {
        String value = event.value();
        if (value == null || value.isEmpty()) {
            // value 为 null 表示 tombstone（删除事件的旧 key 清理），跳过
            return;
        }

        log.debug("[Debezium-Raw] {}", value);  // DEBUG 级别，需要调低 logback 级别才打印

        try {
            JsonNode root = MAPPER.readTree(value);

            // Debezium 内嵌引擎用 JsonConverter（Json.class）输出的是 Connect 标准格式：
            //   { "schema": {...}, "payload": { "op": "...", "source": {...}, "before": {...}, "after": {...} } }
            // op / source / before / after 都在 payload 层，而不是顶层。
            // 这与 Flink 版 JsonDebeziumDeserializationSchema 输出的扁平 envelope 不同。
            JsonNode payload = root.path("payload");
            if (payload.isMissingNode()) {
                // 非标准事件（如 Heartbeat），无 payload，直接跳过
                log.debug("[Debezium-Skip] 无 payload 的事件，跳过: {}", value);
                return;
            }

            String op = payload.path("op").asText("");
            JsonNode source = payload.path("source");
            String db = source.path("db").asText("unknown");
            String table = source.path("table").asText("unknown");
            JsonNode before = payload.path("before");
            JsonNode after = payload.path("after");

            // 心跳事件等无 op 的消息不打印 WARN，避免刷屏
            if (op.isEmpty()) {
                log.debug("[Debezium-Skip] 无 op 的事件（心跳/DDL），跳过: {}", value);
                return;
            }

            switch (op) {
                case "c": // create → INSERT
                    handleInsert(db, table, after);
                    break;
                case "u": // update → UPDATE（同时有 before 和 after）
                    handleUpdate(db, table, before, after);
                    break;
                case "d": // delete → DELETE（有 before，after 为 null）
                    handleDelete(db, table, before);
                    break;
                case "r": // read → 全量快照阶段的数据，等价于 INSERT
                    handleInsert(db, table, after);
                    break;
                default:
                    log.warn("[UNKNOWN_OP] db={}, table={}, op='{}', raw={}", db, table, op, value);
            }
        } catch (IOException e) {
            log.error("解析 Debezium JSON 事件失败: {}", value, e);
        }
    }

    // ==================== 业务处理方法 ====================

    /** 处理 INSERT 操作（含全量快照阶段的 READ 操作）。 */
    private void handleInsert(String db, String table, JsonNode after) {
        log.info("[Debezium-INSERT] db={}, 表={}, 新行={}", db, table, after);
        // TODO: 在此填充你的新增业务逻辑
    }

    /** 处理 UPDATE 操作，同时拿到修改前和修改后。 */
    private void handleUpdate(String db, String table, JsonNode before, JsonNode after) {
        log.info("[Debezium-UPDATE] db={}, 表={}, 旧值={}, 新值={}", db, table, before, after);
        // TODO: 在此填充你的修改业务逻辑
    }

    /** 处理 DELETE 操作。 */
    private void handleDelete(String db, String table, JsonNode before) {
        log.info("[Debezium-DELETE] db={}, 表={}, 旧行={}", db, table, before);
        // TODO: 在此填充你的删除业务逻辑
    }

    // ==================== Debezium 配置构建 ====================

    /**
     * 构建 Debezium MySQL Connector Properties。
     * <p>
     * 所有属性参考 Debezium 官方文档：
     * <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html">
     * MySQL Connector Configuration</a>
     * </p>
     */
    private Properties buildDebeziumProperties() {
        Properties props = new Properties();

        // 基础配置
        props.setProperty("name", SERVER_NAME);
        props.setProperty("connector.class", MySqlConnector.class.getName());
        props.setProperty("tasks.max", "1");  // 单任务，简单场景足够

        // MySQL 连接
        props.setProperty("database.hostname", HOST);
        props.setProperty("database.port", String.valueOf(PORT));
        // PORT 现在是 int，上面的 String.valueOf 保留（Debezium 属性统一为 String）
        props.setProperty("database.user", USERNAME);
        props.setProperty("database.password", PASSWORD);

        // 实例标识（全局唯一）
        props.setProperty("database.server.id", SERVER_ID);
        props.setProperty("database.server.name", SERVER_NAME);

        // 监听范围（Debezium 的 include 是 Java 正则）
        props.setProperty("database.include.list", DATABASE);
        props.setProperty("table.include.list", DATABASE + "\\.eb_product");

        // Offset 存储（binlog 消费位点）
        // MemoryOffsetBackingStore：纯 HashMap 实现，零磁盘依赖，Pod 重启从最新 binlog 位点重开
        // FileOffsetBackingStore：纯文件实现，重启可断点续传（需要挂载 PVC）
        // KafkaOffsetBackingStore：真的连 Kafka，我们不用
        props.setProperty("offset.storage",
                "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        props.setProperty("offset.flush.interval.ms", "60000");

        // 历史记录存储（schema history）
        // MemoryDatabaseHistory：进程内存储，零磁盘依赖，Pod 重启从最新 binlog 位点重开
        // FileDatabaseHistory：纯文件实现，持久化 schema 变更（需要挂载 PVC）
        props.setProperty("database.history",
                "io.debezium.relational.history.MemoryDatabaseHistory");

        // 快照模式
        // schema_only：跳过全量快照，直接从最新 binlog 位点开始监听
        //   — 与内存存储搭配最合理（两者都是 Pod 重启丢状态）
        // initial：做全量快照再切 binlog（需要表有主键）
        props.setProperty("snapshot.mode", "schema_only");

        // 其他常用配置
        props.setProperty("include.schema.changes", "false");  // 不关心 DDL 变更
        props.setProperty("decimal.handling.mode", "string");   // DECIMAL 用字符串避免精度丢失
        props.setProperty("time.precision.mode", "connect");    // 时间精度跟 MySQL 一致
        props.setProperty("tombstones.on.delete", "false");     // 删除时不额外发一条 tombstone 消息
        props.setProperty("heartbeat.interval.ms", "5000");     // 心跳间隔，防止长时间无变更时 binlog 断连
        props.setProperty("max.queue.size", "8192");           // 内部队列大小
        props.setProperty("max.batch.size", "2048");            // 每次批处理最大事件数

        // JDBC 连接参数
        props.setProperty("database.connection.timeout.ms", "10000");
        props.setProperty("database.connection.pool.size", "5");

        return props;
    }
}
