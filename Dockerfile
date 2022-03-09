FROM openjdk:16-jdk
RUN mkdir /app
COPY ./build/install/pagerank /app/
WORKDIR /app/bin
CMD ["./pagerank"]