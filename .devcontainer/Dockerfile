# [Choice] Ubuntu version (use jammy or bionic on local arm64/Apple Silicon): jammy, focal, bionic
ARG VARIANT="bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}
# FROM ubuntu:20.04
# Options for setup script
# ARG INSTALL_ZSH="true"
# ARG UPGRADE_PACKAGES="true"
ADD sources.list /etc/apt/
RUN apt-get update && apt-get -y install tree vim tmux python3-pip gcc cmake build-essential git gdb