package com.zzq.listener;

import com.zzq.cdc.FlinkChangeDataCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 */
@Component
public class ApplicationRefreshedListener {

    private static final Logger log = LoggerFactory.getLogger(ApplicationRefreshedListener.class);

    /**
     * Spring Boot 完全就绪后触发，拉起 Flink CDC 监听任务。
     * <p>
     * 注意：{@link FlinkChangeDataCapture#start()} 内部 env.execute() 是阻塞调用，
     * 必须放到独立线程中执行，否则会阻塞 Spring 启动主线程导致 Tomcat 无法启动。
     * </p>
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        log.info("========== Spring Boot 启动完成，拉起后置任务 ==========");

        startFlinkCdc();

        // 以后有其他启动后置任务，可在此追加
        // startRocketMqConsumer();
        // warmUpCache();
    }

    /**
     * 启动 Flink CDC 监听 MySQL jjjy181 库的增删改变更。
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
}
