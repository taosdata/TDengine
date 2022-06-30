FROM tdengine/tdengine-beta:latest

ENV DEBIAN_FRONTEND=noninteractive
ARG MIRROR=archive.ubuntu.com
RUN sed -Ei 's/\w+.ubuntu.com/'${MIRROR}'/' /etc/apt/sources.list && apt update && apt install mono-devel -y
RUN apt-get install wget -y \
  && wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb \
  && apt-get update && apt-get install -y dotnet-sdk-5.0
COPY ./*.cs *.csproj /tmp/
WORKDIR /tmp/
RUN dotnet build -c Release && cp bin/Release/net5.0/taosdemo bin/Release/net5.0/taosdemo.* /usr/local/bin/ && rm -rf /tmp/*

FROM tdengine/tdengine-beta:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install wget -y \
  && wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb \
  && apt-get update && apt-get install -y dotnet-runtime-5.0
COPY --from=0 /usr/local/bin/taosdemo* /usr/local/bin/
CMD ["/usr/local/bin/taosdemo"]
