# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.177.0/containers/java/.devcontainer/base.Dockerfile

# [Choice] Java version: 11, 15
ARG VARIANT="20"
FROM eclipse-temurin:${VARIANT}-jdk-jammy

# [Option] Install zsh
ARG INSTALL_ZSH="true"
# [Option] Upgrade OS packages to their latest versions
ARG UPGRADE_PACKAGES="false"
# [Option] Enable non-root Docker access in container
ARG ENABLE_NONROOT_DOCKER="true"
# [Option] Use the OSS Moby CLI instead of the licensed Docker CLI
ARG USE_MOBY="true"

# Install needed packages and setup non-root user.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG USER_PASSWORD=random

ARG SSH_PORT=2222

# Copy custom CA certificates
COPY .work/*.crt /usr/local/share/ca-certificates/

COPY library-scripts/*.sh /tmp/library-scripts/
RUN update-ca-certificates \
    && apt-get update \
    && /bin/bash /tmp/library-scripts/common-debian.sh "${INSTALL_ZSH}" "${USERNAME}" "${USER_UID}" "${USER_GID}" "${UPGRADE_PACKAGES}" "true" "true" \
    # Use Docker script from script library to set things up
    && /bin/bash /tmp/library-scripts/docker-in-docker-debian.sh "${ENABLE_NONROOT_DOCKER}" "${USERNAME}" "${USE_MOBY}" \
    # Install the SSH daemon
    && /bin/bash /tmp/library-scripts/sshd-debian.sh "${SSH_PORT}" "${USERNAME}" "false" "${USER_PASSWORD}" "true" \
    # Install the Azure CLI
    && bash /tmp/library-scripts/azcli-debian.sh \
    # Clean up
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/library-scripts/ \
    # Trust the GitHub public RSA key
    # This key was manually validated by running 'ssh-keygen -lf <key-file>' and comparing the fingerprint to the one found at:
    # https://docs.github.com/en/github/authenticating-to-github/githubs-ssh-key-fingerprints
    && mkdir -p /home/${USERNAME}/.ssh \
    && echo "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl" >> /home/${USERNAME}/.ssh/known_hosts \
    && echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" >> /home/${USERNAME}/.ssh/known_hosts \
    && echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" >> /home/${USERNAME}/.ssh/known_hosts \
    && chown -R ${USERNAME} /home/${USERNAME}/.ssh \
    && touch /usr/local/share/bash_history \
    && chown ${USERNAME} /usr/local/share/bash_history

# Add custom CA certificates to Java truststore
RUN true \
    # Any command which returns non-zero exit code will cause this shell script to exit immediately:
    && set -e \
    # Activate debugging to show execution details: all commands will be printed before execution
    &&  set -x \
    # Figure out where java is installed
    && JAVA_DIR=$(readlink -f "$(dirname $(which java))/../") \
    && for cert in /usr/local/share/ca-certificates/*; do \
        openssl x509 -outform der -in "$cert" -out /tmp/certificate.der; \
        ${JAVA_DIR}/bin/keytool -import -alias "$cert" -keystore ${JAVA_DIR}/lib/security/cacerts -file /tmp/certificate.der -deststorepass changeit -noprompt; \
    done \
    && rm /tmp/certificate.der

# Setting the ENTRYPOINT to docker-init.sh will configure non-root access to
# the Docker socket if "overrideCommand": false is set in devcontainer.json.
# The script will also execute CMD if you need to alter startup behaviors.
ENTRYPOINT [ "/usr/local/share/ssh-init.sh", "/usr/local/share/docker-init.sh" ]
CMD [ "sleep", "infinity" ]

# Use the Maven cache from the host and persist Bash history
RUN mkdir -p /usr/local/share/m2 \
    && chown -R ${USER_UID}:${USER_GID} /usr/local/share/m2 \
    && ln -s /usr/local/share/m2 /home/${USERNAME}/.m2

ARG MAVEN_VERSION=""
ARG TRINO_VERSION="426"

# Install Maven
RUN mkdir -p /tmp \
    # Install additional OS packages.
    && apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends bash-completion vim maven \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* \
    # Install Trino CLI
    && wget https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar -P /usr/local/bin \
    && chmod +x /usr/local/bin/trino-cli-${TRINO_VERSION}-executable.jar \
    && ln -s /usr/local/bin/trino-cli-${TRINO_VERSION}-executable.jar /usr/local/bin/trino

ENV M2_HOME=/opt/maven
ENV MAVEN_HOME=/opt/maven
ENV PATH=${M2_HOME}/bin:${PATH}
