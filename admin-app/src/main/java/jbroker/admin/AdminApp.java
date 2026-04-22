package jbroker.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Phase 8 admin web app. Hosts the REST API defined in PRD §8.7, the SSE
 * event stream (§8.8), and the Thymeleaf UI pages consumed by operators.
 *
 * <p>P8.1 boots the empty app with a health endpoint so the rest of Phase 8
 * has a running harness to layer controllers into. Subsequent slices add:
 * <ul>
 *   <li>P8.2 — {@code /api/v1/cluster} + {@code /api/v1/nodes} controllers</li>
 *   <li>P8.3 — {@code /api/v1/topics} CRUD</li>
 *   <li>P8.4 — {@code /api/v1/consumer-groups}</li>
 *   <li>P8.5 — {@code /api/v1/raft} + {@code /metrics/*}</li>
 *   <li>P8.6 — {@code /api/v1/events} SSE stream</li>
 *   <li>P8.7+P8.8 — Thymeleaf pages under {@code /}</li>
 * </ul>
 */
@SpringBootApplication
public class AdminApp {
    public static void main(String[] args) {
        SpringApplication.run(AdminApp.class, args);
    }
}
