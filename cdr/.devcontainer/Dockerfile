FROM ros:jazzy as base

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
	--mount=type=cache,target=/var/lib/apt,sharing=locked \
	apt update \
	&& apt-get install --yes --no-install-recommends \
		dnsutils \
		ethtool \
		htop \
		inetutils-ping \
		inetutils-traceroute \
		iproute2 \
		jq \
		nmap \
		net-tools \
		rsync \
		software-properties-common \
		sudo \
		ssh \
		strace \
		tcpdump \
		unzip \
		vim \
		wget \
		zip \
		zsh



RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
	--mount=type=cache,target=/var/lib/apt,sharing=locked \
	curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
	&& apt-get install --yes --no-install-recommends \
		nodejs

RUN VERSION="1.25.5"; \
	ARCH=$(dpkg --print-architecture); \
	cd "${TEMP}" \
	&& curl -fsSL "https://golang.google.cn/dl/go${VERSION}.linux-${ARCH}.tar.gz" \
		| tar -xzf - -C /usr/local



ARG USERNAME=hypnos
ARG USER_UID=${UID:-1000}
ARG USER_GID=${GID:-${USER_UID}}
RUN EXISTING_GROUP=$(getent group ${USER_GID} | cut -d: -f1) && \
	EXISTING_USER=$(getent passwd ${USER_UID} | cut -d: -f1) && \
	if [ -n "$EXISTING_GROUP" ]; then \
		groupmod -n "${USERNAME}" "$EXISTING_GROUP"; \
	else \
		groupadd "${USERNAME}" --gid ${USER_GID}; \
	fi && \
	if [ -n "$EXISTING_USER" ]; then \
		usermod -l "${USERNAME}" -d "/home/${USERNAME}" -m "$EXISTING_USER" && \
		groupmod -n "${USERNAME}" "$EXISTING_USER" 2>/dev/null || true; \
	else \
		useradd --create-home "${USERNAME}" --shell /bin/bash --uid "${USER_UID}" --gid "${USER_GID}"; \
	fi && \
	echo "${USERNAME}:${USERNAME}" | chpasswd && \
	echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/${USERNAME}"



USER hypnos

ENV GOROOT=/usr/local/go
ENV GOPATH=/home/hypnos/go
ENV PATH="$GOROOT/bin:$PATH"

# Comment out the following lines if you don't want to use Zsh.
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true \
	&& git clone https://github.com/zsh-users/zsh-autosuggestions         ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions     \
	&& git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting \
	&& git clone https://github.com/zsh-users/zsh-completions             ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-completions
COPY --chown=1000:1000 .zshrc /home/hypnos
ENV SHELL=/bin/zsh

RUN mkdir -p \
		~/.cache/go-build \
		~/.cache/uv
