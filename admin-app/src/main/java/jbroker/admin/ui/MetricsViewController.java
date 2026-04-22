package jbroker.admin.ui;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Thymeleaf view controller for the metrics page. The template loads
 * Chart.js, then polls {@code /api/v1/metrics/throughput} and
 * {@code /api/v1/metrics/latency} every 2 s to refresh two line charts
 * (produce vs fetch throughput in B/s, produce vs fetch p99 in µs).
 */
@Controller
public class MetricsViewController {

    @GetMapping("/metrics")
    public String metrics() {
        return "metrics";
    }
}
