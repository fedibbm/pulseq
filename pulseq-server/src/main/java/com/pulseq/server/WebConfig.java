package com.pulseq.server;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Serves the built Angular dashboard (see {@code pulseq-dashboard/}) straight from the
 * filesystem, so the broker API, WebSocket endpoint and dashboard all share one origin.
 *
 * <p>Resolves {@code pulseq.dashboard-path} against the working directory or uses it as-is
 * when absolute. When the directory is absent (dashboard not built, plain library use) no
 * handler is registered and the default Spring static handling applies.</p>
 */
@Configuration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class WebConfig implements WebMvcConfigurer {

    private final String dashboardPath;

    public WebConfig(@Value("${pulseq.dashboard-path:}") String dashboardPath) {
        this.dashboardPath = dashboardPath;
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        if (dashboardPath == null || dashboardPath.isBlank()) {
            return;
        }
        Path dir = Paths.get(dashboardPath).toAbsolutePath().normalize();
        if (!Files.isDirectory(dir)) {
            return;
        }
        registry.addResourceHandler("/**")
                .addResourceLocations("file:" + dir + "/")
                .setCachePeriod(0);
    }
}
