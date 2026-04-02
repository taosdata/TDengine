cmake_minimum_required(VERSION 3.16)                                                                                                                                        
                                                                                
project(crashdump)                                                              
                                                                                
add_executable(dumper dumper/dumper.c)                                          
target_link_libraries(dumper Dbghelp)                                           
install(TARGETS dumper)                                                         
                                                                                
add_library(crashdump STATIC crasher/crasher.c)                                 
install(TARGETS crashdump)                                                      
