#!/bin/bash
# ============================================================================
# modules/java.sh — JDK + Maven toolchain configuration
# ============================================================================

mod_java_check() {
    header "Java Toolchain"

    # JDK
    if cmd_exists java; then
        local ver
        ver=$(java -version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        # Java 17+ reports as "17.0.x", older as "1.8.0_xxx"
        local major
        major=$(echo "$ver" | cut -d. -f1)
        if [[ "$major" -ge "$REQUIRED_JAVA_VERSION" ]] 2>/dev/null; then
            ok "java $ver (major $major >= $REQUIRED_JAVA_VERSION)"
        else
            warn "java $ver (need major >= $REQUIRED_JAVA_VERSION)"
            ISSUES_FOUND=$((ISSUES_FOUND + 1))
        fi
    else
        fail "java not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi

    # Maven
    if cmd_exists mvn; then
        ok "mvn $(mvn --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    else
        warn "mvn not found"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
}

mod_java_install() {
    # JDK
    if ! cmd_exists java; then
        if confirm "Install OpenJDK ${REQUIRED_JAVA_VERSION}?"; then
            case "$PKG_MGR" in
                brew) brew install "openjdk@${REQUIRED_JAVA_VERSION}" ;;
                apt)  pkg_install "openjdk-${REQUIRED_JAVA_VERSION}-jdk" ;;
                yum|dnf) pkg_install "java-${REQUIRED_JAVA_VERSION}-openjdk-devel" ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi

    # Maven
    if ! cmd_exists mvn; then
        if confirm "Install Maven?"; then
            case "$PKG_MGR" in
                brew) brew install maven ;;
                apt)  pkg_install maven ;;
                yum|dnf) pkg_install maven ;;
            esac
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}

mod_java_config() {
    if [[ "${TSDB_PUBLIC_DEPS:-0}" == "1" ]]; then
        ok "Public mode: using Maven Central"
        return 0
    fi

    # Maven settings.xml — configure internal Nexus mirror if available
    local mvn_settings="$HOME/.m2/settings.xml"
    # MAVEN_MIRROR_URL is set by config.sh from .build-args
    local nexus_maven_url="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"

    if [[ -f "$mvn_settings" ]] && grep -qF "nexus.tdengine.net" "$mvn_settings"; then
        return 0
    fi

    if confirm "Configure Maven mirror → internal Nexus in $mvn_settings?"; then
        mkdir -p "$HOME/.m2"
        backup_file "$mvn_settings"
        cat > "$mvn_settings" <<MVN_EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0
                              https://maven.apache.org/xsd/settings-1.2.0.xsd">
  <mirrors>
    <mirror>
      <id>nexus-tdengine</id>
      <mirrorOf>central</mirrorOf>
      <name>TDengine Internal Nexus</name>
      <url>${nexus_maven_url}</url>
    </mirror>
  </mirrors>
</settings>
MVN_EOF
        ok "Maven settings written to $mvn_settings"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
