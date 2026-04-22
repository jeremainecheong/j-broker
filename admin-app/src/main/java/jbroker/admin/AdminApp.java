package jbroker.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Milestone 8 admin web app. Hosts the REST API defined in the spec, the SSE
 * event stream (§8.8), and the Thymeleaf UI pages consumed by operators.
 *
 * <p>boots the empty app with a health endpoint so the rest of Milestone 8
 * has a running harness to layer controllers into. Subsequent slices add:
 * <ul>
 *   <li>{@code /api/v1/cluster} + {@code /api/v1/nodes} controllers</li>
 *   <li>{@code /api/v1/topics} CRUD</li>
 *   <li>{@code /api/v1/consumer-groups}</li>
 *   <li>{@code /api/v1/raft} + {@code /metrics/*}</li>
 *   <li>{@code /api/v1/events} SSE stream</li>
 *   <li>+Thymeleaf pages under {@code /}</li>
 * </ul>
 */
@SpringBootApplication
public class AdminApp {
    public static void main(String[] args) {
        SpringApplication.run(AdminApp.class, args);
    }
}
