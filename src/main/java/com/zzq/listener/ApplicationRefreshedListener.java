package com.zzq.listener;

import com.zzq.cdc.DebeziumChangeDataCapture;
import com.zzq.cdc.FlinkChangeDataCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Spring Boot 启动完成后的后置任务触发监听器。
 * <p>
 * 使用 {@link ApplicationReadyEvent}（而非 ContextRefreshedEvent），
 * 确保在所有 Bean 初始化完毕、所有初始化器执行完成、Web 容器就绪后才触发，
 * 且 Spring Boot 3.x 默认单层容器，不会重复触发。
 * </p>
 *
 * <h3>CDC 实现切换</h3>
 * <p>
 * 通过常量 {@link #CDC_IMPL} 选择监听实现：
 * </p>
 * <table>
 *   <tr><th>值</th><th>实现类</th><th>说明</th></tr>
 *   <tr><td>{@code "debezium"}</td><td>{@link DebeziumChangeDataCapture}</td><td>纯 Debezium 内嵌引擎，无 Flink 依赖，轻量</td></tr>
 *   <tr><td>{@code "flink"}</td><td>{@link FlinkChangeDataCapture}</td><td>Flink CDC 3.5 DataStream API，功能完整但较重</td></tr>
 * </table>
 * <p>两种实现都监听 MySQL {@code jjjy181} 库的增删改，数据源配置相同。</p>
 */
@Component
public class ApplicationRefreshedListener implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ApplicationRefreshedListener.class);

    /**
     * 当前运行的 Debezium CDC 实例（仅当 {@link #CDC_IMPL} 为 {@code "debezium"} 时非空）。
     * <p>
     * 持有引用是为了应用关闭时能调用 {@link DebeziumChangeDataCapture#stop()}
     * 优雅关闭引擎，避免 MySQL 残留 Binlog Dump 连接。
     * </p>
     */
    private volatile DebeziumChangeDataCapture debeziumCdc;

    /**
     * CDC 监听实现切换开关：{@code "debezium"} 或 {@code "flink"}。
     * <p>
     * 改完 IDE 里直接重启即可生效，不需要动 pom.xml。
     * </p>
     */
    private static final String CDC_IMPL = "debezium";

    /**
     * Spring Boot 完全就绪后触发，拉起 CDC 监听任务。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        log.info("========== Spring Boot 启动完成，拉起后置任务 ==========");

        switch (CDC_IMPL) {
            case "flink":
                startFlinkCdc();
                break;
            case "debezium":
            default:
                startDebeziumCdc();
                break;
        }
    }

    /**
     * 启动 Flink CDC 3.5 DataStream API 监听。
     */
    private void startFlinkCdc() {
        new Thread(() -> {
            try {
                new FlinkChangeDataCapture().start();
            } catch (Exception e) {
                log.error("Flink CDC 启动或运行期间发生异常", e);
            }
        }, "flink-cdc-worker").start();
    }

    /**
     * 启动 Debezium 内嵌引擎 CDC 监听（更轻量，不依赖 Flink）。
     */
    private void startDebeziumCdc() {
        DebeziumChangeDataCapture cdc = new DebeziumChangeDataCapture();
        this.debeziumCdc = cdc; // 持有引用，供 destroy() 优雅关闭
        Thread worker = new Thread(() -> {
            try {
                cdc.start();
            } catch (Exception e) {
                log.error("Debezium CDC 启动或运行期间发生异常", e);
            }
        }, "debezium-cdc-worker");
        worker.setDaemon(false); // 非 daemon：确保应用关闭前引擎有机会被 stop() 优雅关闭
        worker.start();
    }

    /**
     * 应用关闭时优雅关闭 CDC 引擎，释放 MySQL Binlog Dump 连接。
     */
    @Override
    public void destroy() {
        if (debeziumCdc != null) {
            log.info("应用关闭，优雅停止 Debezium CDC 引擎...");
            debeziumCdc.stop();
        }
    }
}
