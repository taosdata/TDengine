cmake_minimum_required(VERSION 3.16)

project(wcwidth)

add_library(wcwidth STATIC wcwidth.c)
INSTALL(TARGETS wcwidth)
