FROM openjdk:16-jdk
RUN mkdir /app
COPY ./pagerank /app
RUN cd /app && chmod +x gradlew && ./gradlew build
WORKDIR /app
RUN touch .env
CMD ["./gradlew", "run"]