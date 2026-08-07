package com.pulseq.server;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Serves the dashboard shell at the root. The dashboard is a single-page static app with no
 * client-side routes, so forwarding to index.html is all the SPA fallback that is needed.
 */
@Controller
public class DashboardController {

    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public String index() {
        return "forward:/index.html";
    }
}
