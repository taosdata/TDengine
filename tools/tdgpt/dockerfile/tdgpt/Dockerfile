FROM tdgpt_env:1.0
WORKDIR /apps
ENV DEBIAN_FRONTEND=noninteractive
ARG pkgFile
ARG dirName
ADD ${pkgFile} /apps
RUN cd ${dirName}/ && /bin/bash install.sh -e no && cd .. && rm -rf ${dirName}
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
EXPOSE 6090 8387
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
