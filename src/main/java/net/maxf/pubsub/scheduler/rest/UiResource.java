package net.maxf.pubsub.scheduler.rest;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@Path("/")
@Tag(name = "UI", description = "Scheduler UI")
public class UiResource {

    private static final String STATIC_DIR = "/app/static";

    @GET
    @Produces("text/html")
    @Operation(hidden = true)
    public Response index() {
        return serveFile("index.html", "text/html");
    }

    @GET
    @Path("{path:.*}")
    @Operation(hidden = true)
    public Response serve(@PathParam("path") String path) {
        if (path == null || path.isEmpty()) {
            return index();
        }

        // Prevent directory traversal
        if (path.contains("..")) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        String contentType = getContentType(path);
        Response response = serveFile(path, contentType);

        // For SPA: if file not found and not an API/asset request, serve index.html
        if (response.getStatus() == 404 && !path.startsWith("api/") && !path.startsWith("q/") && !hasExtension(path)) {
            return index();
        }

        return response;
    }

    private Response serveFile(String path, String contentType) {
        // Try filesystem first (Docker deployment)
        java.nio.file.Path filePath = Paths.get(STATIC_DIR, path);
        if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
            try {
                byte[] content = Files.readAllBytes(filePath);
                return Response.ok(content, contentType).build();
            } catch (IOException e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }
        }

        // Try classpath (dev mode)
        try (InputStream is = getClass().getResourceAsStream("/META-INF/resources/" + path)) {
            if (is != null) {
                return Response.ok(is.readAllBytes(), contentType).build();
            }
        } catch (IOException e) {
            // Fall through to 404
        }

        return Response.status(Response.Status.NOT_FOUND).build();
    }

    private String getContentType(String path) {
        if (path.endsWith(".html")) return "text/html";
        if (path.endsWith(".css")) return "text/css";
        if (path.endsWith(".js")) return "application/javascript";
        if (path.endsWith(".json")) return "application/json";
        if (path.endsWith(".png")) return "image/png";
        if (path.endsWith(".jpg") || path.endsWith(".jpeg")) return "image/jpeg";
        if (path.endsWith(".svg")) return "image/svg+xml";
        if (path.endsWith(".ico")) return "image/x-icon";
        if (path.endsWith(".woff")) return "font/woff";
        if (path.endsWith(".woff2")) return "font/woff2";
        return "application/octet-stream";
    }

    private boolean hasExtension(String path) {
        int lastSlash = path.lastIndexOf('/');
        int lastDot = path.lastIndexOf('.');
        return lastDot > lastSlash && lastDot < path.length() - 1;
    }
}
