# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.177.0/containers/java/.devcontainer/base.Dockerfile

ARG BASE_IMAGE_TAG
FROM uchimera.azurecr.io/cccs/dev/trino-build:${BASE_IMAGE_TAG}

COPY .work/*.crt /usr/local/share/ca-certificates/

ARG USERNAME
USER root
RUN <<EOT
    update-ca-certificates
    for crt in /usr/local/share/ca-certificates/*.crt; do
        alias=$(basename "$crt" .crt)
        keytool -importcert -noprompt -trustcacerts \
            -keystore /opt/java/openjdk/lib/security/cacerts \
            -storepass changeit \
            -alias "$alias" \
            -file "$crt"
    done
    # Set up USER with passwordless sudo
    usermod -a -G sudo ${USERNAME}
    echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" >/etc/sudoers.d/${USERNAME}
    chmod 0440 /etc/sudoers.d/${USERNAME}
EOT

USER ${USERNAME}
RUN <<EOT
    # Trust the GitHub public RSA key
    # This key was manually validated by running 'ssh-keygen -lf <key-file>' and comparing the fingerprint to the one found at:
    # https://docs.github.com/en/github/authenticating-to-github/githubs-ssh-key-fingerprints
    mkdir -p ~/.ssh
    echo "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl" >> ~/.ssh/known_hosts
    echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" >> ~/.ssh/known_hosts
    echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" >> ~/.ssh/known_hosts
EOT
