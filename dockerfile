FROM openjdk:8-jre-alpine
RUN mkdir -p /opt/app
WORKDIR /opt/app
COPY ./runjar.sh ./target/scala-2.13/nautiyal_raj_courseproject-assembly-0.1.jar ./
RUN ["chmod", "+x", "./runjar.sh"]
ENTRYPOINT ["./runjar.sh"]
