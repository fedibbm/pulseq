# Runtime image for the PulseQ server, bundling the built Angular dashboard.
# Build with:  mvn package && docker compose up -d --build
FROM node:22-alpine AS dashboard
WORKDIR /dash
COPY pulseq-dashboard/package*.json ./
RUN npm ci
COPY pulseq-dashboard/ ./
RUN npm run build

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY pulseq-server/target/pulseq-server-0.1.0.jar app.jar
COPY --from=dashboard /dash/dist/pulseq-dashboard/browser /app/dashboard
ENV PULSEQ_DASHBOARD_PATH=/app/dashboard
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
