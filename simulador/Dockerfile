FROM ubuntu:22.04

# Install scala
RUN apt update
RUN apt upgrade -y
RUN apt install -y openjdk-8-jdk curl git unzip
RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup -y
ENV PATH="${PATH}:/root/.local/share/coursier/bin"

# Clone and build eventsim
RUN git clone https://github.com/UTDT-TD7/eventsim.git
WORKDIR eventsim
RUN sbt assembly
RUN chmod +x bin/eventsim

# Execute simulation TODO: move this
CMD [ "bin/eventsim", "-c" , "examples/example-config.json", "--from", "365", "--nusers", "1000", "--growth-rate", "0.03", "--kafkaBrokerList", "172.25.0.2:9092", "--continuous"]
