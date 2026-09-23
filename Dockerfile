FROM maven:3.9.11-eclipse-temurin-21 AS build
ARG APP
WORKDIR /workspace
COPY . .
RUN mvn -B -pl applications/${APP} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG APP
WORKDIR /app
COPY --from=build /workspace/applications/${APP}/target/${APP}-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
