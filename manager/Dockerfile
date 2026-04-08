FROM maven:3.9.8-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml ./
COPY common/pom.xml common/pom.xml
COPY manager/pom.xml manager/pom.xml
COPY worker/pom.xml worker/pom.xml

COPY common/src common/src
COPY manager/src manager/src
COPY worker/src worker/src

RUN mvn -pl manager -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/manager/target/*.jar app.jar
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
