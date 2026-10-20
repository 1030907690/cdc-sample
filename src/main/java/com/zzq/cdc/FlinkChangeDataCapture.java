package com.zzq.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink CDC 核心监听类：通过 Flink CDC 3.5 DataStream API 监听 MySQL jjjy181 库的增删改操作。
 * <p>
 * 运行环境：Flink 1.20.3 + Flink CDC 3.5.0
 * </p>
 * <p>
 * 本类为纯 Flink CDC 逻辑，无 Spring 依赖，可独立运行或被 Spring 启动触发。
 * </p>
 *
 * <h3>技术选型</h3>
 * <ul>
 *   <li>{@link MySqlSource}（泛型 Source，实现 Flink 新 Source API）</li>
 *   <li>{@link StartupOptions} 启动策略（initial=全量快照+增量，latest=只监听启动后变更）</li>
 *   <li>{@link JsonDebeziumDeserializationSchema} 将 Debezium 事件转为 JSON 字符串</li>
 *   <li>Jackson 解析 JSON envelope，op 字段：c=INSERT / u=UPDATE / d=DELETE / r=READ（全量快照）</li>
 * </ul>
 *
 * <h3>注意点</h3>
 * <p>Flink 要求所有算子（ProcessFunction / MapFunction 等）必须可序列化。
 * 因此 CDC 事件分发器设计为 {@code static} 内部类，避免隐式持有外部类引用导致序列化失败。</p>
 */
public class FlinkChangeDataCapture {

    private static final Logger log = LoggerFactory.getLogger(FlinkChangeDataCapture.class);

    // ====== MySQL 连接配置常量 ======
    private static final String HOST     = "192.168.10.131";
    private static final int    PORT     = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String DATABASE = "jjjy181";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 启动 CDC 监听任务（阻塞方法，必须在独立线程中调用）。
     * <p>
     * API 用法参考 Flink CDC 官方文档：
     * <a href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/zh/docs/connectors/flink-sources/overview/#usage-for-datastream-api">
     * Usage for DataStream API</a>
     * </p>
     *
     * @throws Exception Flink 作业启动或运行期间的异常
     */
    public void start() throws Exception {
        // 1. 创建执行环境
        //    createLocalEnvironment(conf) 显式指定本地模式。
        //    如果用 getExecutionEnvironment(conf)，Flink 会 SPI 找 ExecutorFactory，
        //    本地单独跑 Java 程序时找不到，会报 "No ExecutorFactory found"。
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        // 2. Checkpoint + 重启策略（CDC 作业必备，保证断点续传）
        env.enableCheckpointing(60_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2_000L));

        // 3. 构建 MySqlSource（Flink CDC 3.5 DataStream API 官方写法）
        //    注意：CDC 3.5 构造函数强制 checkNotNull(databaseList/tableList/serverTimeZone/dbzProperties)，
        //    即使注释说"默认监听所有表"也必须显式传值。
        //    监听范围限定为 eb_product 表（有主键），避免 initial() 模式下无主键表报 chunk key 错误。
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(HOST)
                .port(PORT)
                .username(USERNAME)
                .password(PASSWORD)
                .databaseList(DATABASE)
                .tableList(DATABASE + ".eb_product")   // 精确监听 eb_product 表
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())  // initial=全量快照 + 增量 binlog
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 4. 注册 Source 为 DataStream（新 Source API 用 env.fromSource）
        DataStreamSource<String> stream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );

        // 5. 按操作类型分发处理（使用 static 内部类，Flink 算子必须可序列化）
        stream.process(new CdcEventDispatcher());

        // 6. 启动作业（阻塞，env.execute() 会常驻直到被取消）
        log.info("Flink CDC 作业启动中，监听 MySQL {}:{} / {} ...", HOST, PORT, DATABASE);
        env.execute("jjjy181 CDC Listener");
    }

    // ==================== 业务处理方法（static，供内部类调用） ====================

    /** 处理 INSERT 操作（含全量快照阶段的 READ 操作）。 */
    private static void handleInsert(String db, String table, JsonNode after) {
        log.info("[INSERT] db={}, 表={}, 新行={}", db, table, after);
        // TODO: 在此填充你的新增业务逻辑
    }

    /** 处理 UPDATE 操作，同时拿到修改前和修改后。 */
    private static void handleUpdate(String db, String table, JsonNode before, JsonNode after) {
        log.info("[UPDATE] db={}, 表={}, 旧值={}, 新值={}", db, table, before, after);
        // TODO: 在此填充你的修改业务逻辑
    }

    /** 处理 DELETE 操作。 */
    private static void handleDelete(String db, String table, JsonNode before) {
        log.info("[DELETE] db={}, 表={}, 旧行={}", db, table, before);
        // TODO: 在此填充你的删除业务逻辑
    }

    // ==================== static 内部类：CDC 事件分发器 ====================
    //
    // 为什么是 static？
    // Flink 在提交作业时会序列化所有算子。非 static 内部类（包括 lambda 和匿名类）
    // 会隐式持有外部类引用，导致外部类也要可序列化，而外部类通常不可序列化。
    // static 内部类不持有外部类引用，自身只需序列化自身字段。
    //
    // 为什么不抽成独立顶级类？
    // 这里逻辑与 FlinkChangeDataCapture 紧耦合（复用 MAPPER、log、三个 handle 方法），
    // 作为 static 内部类内聚性更好，避免跨文件跳转。

    /**
     * Debezium JSON 事件解析 + 按操作类型分发处理器。
     * <p>
     * JSON envelope 结构：
     * <pre>
     * {
     *   "op":   "c" | "u" | "d" | "r",
     *   "before": {...} | null,   // 修改前/删除前
     *   "after":  {...} | null,   // 修改后/新增
     *   "source": { "db": "...", "table": "...", ... }
     * }
     * </pre>
     */
    private static class CdcEventDispatcher extends ProcessFunction<String, Void> {

        @Override
        public void processElement(String json, Context ctx, Collector<Void> out) {
            try {
                JsonNode root = MAPPER.readTree(json);
                String op = root.path("op").asText("");
                JsonNode source = root.path("source");
                String db = source.path("db").asText("unknown");
                String table = source.path("table").asText("unknown");
                JsonNode before = root.path("before");
                JsonNode after = root.path("after");

                switch (op) {
                    case "c": // create → INSERT
                        handleInsert(db, table, after);
                        break;
                    case "u": // update → UPDATE
                        handleUpdate(db, table, before, after);
                        break;
                    case "d": // delete → DELETE
                        handleDelete(db, table, before);
                        break;
                    case "r": // read → 全量快照阶段，等价于 INSERT
                        handleInsert(db, table, after);
                        break;
                    default:
                        log.debug("[UNKNOWN_OP] db={}, table={}, op={}, raw={}", db, table, op, json);
                }
            } catch (Exception e) {
                log.error("解析 CDC 事件 JSON 失败: {}", json, e);
            }
        }
    }
}
