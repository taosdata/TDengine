FROM tdgpt_env:1.0
WORKDIR /apps
ENV DEBIAN_FRONTEND=noninteractive
ARG pkgFile
ARG dirName
ADD http://192.168.1.131/data/nas/TDengine/anode/taos.pth /apps
ADD http://192.168.1.131/data/nas/TDengine/anode/taos_ts_server.py /apps
ADD ${pkgFile} /apps/
RUN cd ${dirName}/ && /bin/bash install.sh -e no && cd .. && rm -rf ${dirName}
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /apps/taos_ts_server.py
EXPOSE 6090 8387 5000
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
