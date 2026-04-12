FROM maven:3.9.8-eclipse-temurin-17 AS builder

WORKDIR /build

COPY streams-app/pom.xml /build/pom.xml
COPY streams-app/src /build/src

RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /build/target/streams-app-1.0.0.jar /app/streams-app.jar

CMD ["java", "-jar", "/app/streams-app.jar"]
