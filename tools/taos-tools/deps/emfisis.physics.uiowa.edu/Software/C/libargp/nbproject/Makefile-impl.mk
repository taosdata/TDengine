# 
# Generated Makefile - do not edit! 
# 
# Edit the Makefile in the project folder instead (../Makefile). Each target
# has a pre- and a post- target defined where you can add customization code.
#
# This makefile implements macros and targets common to all configurations.
#
# NOCDDL


# Building and Cleaning subprojects are done by default, but can be controlled with the SUB
# macro. If SUB=no, subprojects will not be built or cleaned. The following macro
# statements set BUILD_SUB-CONF and CLEAN_SUB-CONF to .build-reqprojects-conf
# and .clean-reqprojects-conf unless SUB has the value 'no'
SUB_no=NO
SUBPROJECTS=${SUB_${SUB}}
BUILD_SUBPROJECTS_=.build-subprojects
BUILD_SUBPROJECTS_NO=
BUILD_SUBPROJECTS=${BUILD_SUBPROJECTS_${SUBPROJECTS}}
CLEAN_SUBPROJECTS_=.clean-subprojects
CLEAN_SUBPROJECTS_NO=
CLEAN_SUBPROJECTS=${CLEAN_SUBPROJECTS_${SUBPROJECTS}}


# Project Name
PROJECTNAME=libargp

# Active Configuration
DEFAULTCONF=Windows
CONF=${DEFAULTCONF}

# All Configurations
ALLCONFS=Windows 


# build
.build-impl: .validate-impl 
	@#echo "=> Running $@... Configuration=$(CONF)"
	${MAKE} -f nbproject/Makefile-${CONF}.mk SUBPROJECTS=${SUBPROJECTS} .build-conf


# clean
.clean-impl: .validate-impl
	@#echo "=> Running $@... Configuration=$(CONF)"
	${MAKE} -f nbproject/Makefile-${CONF}.mk SUBPROJECTS=${SUBPROJECTS} .clean-conf


# clobber 
.clobber-impl:
	@#echo "=> Running $@..."
	for CONF in ${ALLCONFS}; \
	do \
	    ${MAKE} -f nbproject/Makefile-$${CONF}.mk SUBPROJECTS=${SUBPROJECTS} .clean-conf; \
	done

# all 
.all-impl:
	@#echo "=> Running $@..."
	for CONF in ${ALLCONFS}; \
	do \
	    ${MAKE} -f nbproject/Makefile-$${CONF}.mk SUBPROJECTS=${SUBPROJECTS} .build-conf; \
	done


# configuration validation
.validate-impl:
	@if [ ! -f nbproject/Makefile-${CONF}.mk ]; \
	then \
	    echo ""; \
	    echo "Error: can not find the makefile for configuration '${CONF}' in project ${PROJECTNAME}"; \
	    echo "See 'make help' for details."; \
	    echo "Current directory: " `pwd`; \
	    echo ""; \
	fi
	@if [ ! -f nbproject/Makefile-${CONF}.mk ]; \
	then \
	    exit 1; \
	fi


# help
.help-impl:
	@echo "This makefile supports the following configurations:"
	@echo "    ${ALLCONFS}"
	@echo ""
	@echo "and the following targets:"
	@echo "    build  (default target)"
	@echo "    clean"
	@echo "    clobber"
	@echo "    all"
	@echo "    help"
	@echo ""
	@echo "Makefile Usage:"
	@echo "    make [CONF=<CONFIGURATION>] [SUB=no] build"
	@echo "    make [CONF=<CONFIGURATION>] [SUB=no] clean"
	@echo "    make [SUB=no] clobber"
	@echo "    make [SUB=no] all"
	@echo "    make help"
	@echo ""
	@echo "Target 'build' will build a specific configuration and, unless 'SUB=no',"
	@echo "    also build subprojects."
	@echo "Target 'clean' will clean a specific configuration and, unless 'SUB=no',"
	@echo "    also clean subprojects."
	@echo "Target 'clobber' will remove all built files from all configurations and,"
	@echo "    unless 'SUB=no', also from subprojects."
	@echo "Target 'all' will will build all configurations and, unless 'SUB=no',"
	@echo "    also build subprojects."
	@echo "Target 'help' prints this message."
	@echo ""

