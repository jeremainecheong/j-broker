package jbroker.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Admin web app. Hosts the REST API under {@code /api/v1}, the SSE event
 * stream, and the Thymeleaf UI pages consumed by operators:
 * <ul>
 *   <li>{@code /api/v1/cluster} + {@code /api/v1/nodes} controllers</li>
 *   <li>{@code /api/v1/topics} CRUD</li>
 *   <li>{@code /api/v1/consumer-groups}</li>
 *   <li>{@code /api/v1/raft} + {@code /metrics/*}</li>
 *   <li>{@code /api/v1/events} SSE stream</li>
 *   <li>Thymeleaf pages under {@code /}</li>
 * </ul>
 */
@SpringBootApplication
public class AdminApp {
    public static void main(String[] args) {
        SpringApplication.run(AdminApp.class, args);
    }
}
