package ru.dreamkas.statistics.restpipe;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


@SpringBootApplication
@RestController
public class RestPipeApplication {

    private final RestTemplate template;
    private final AppConfig config;
    private final ObjectMapper om;
    private final URI uri;

    @Autowired
    public RestPipeApplication(
            RestTemplate template,
            AppConfig config,
            ObjectMapper om) throws URISyntaxException {
        this.template = template;
        this.config = config;
        this.om = om;
        this.uri = new URI("http://" + config.getMasterUrl());
    }

    @PostMapping("/**")
    public void input(@RequestBody String body, HttpServletRequest servletRequest) throws IOException {
        Object path = servletRequest.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        template.postForEntity(uri, Request.of(path.toString(), body), String.class);
    }

    private static class Request {
        private final Map<String, Object> headers;
        private final String body;

        private Request(Map<String, Object> headers, String body) {
            this.headers = headers;
            this.body = body;
        }

        public static Request[] of(String path, String body) {
            Map<String, Object> headers = new HashMap<>();
            headers.put("path", path);
            return new Request[]{new Request(headers, body)};
        }
    }

    private static class Header {

    }

    public static void main(String[] args) {
        SpringApplication.run(RestPipeApplication.class, args);
    }
}
