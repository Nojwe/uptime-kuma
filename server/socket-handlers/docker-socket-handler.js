const { sendDockerHostList } = require("../client");
const { checkLogin } = require("../util-server");
const { DockerHost } = require("../docker");
const { log } = require("../../src/util");
const { R } = require("redbean-node");
const axios = require("axios");
const https = require("https");
const http = require("http");
const crypto = require("crypto");

/**
 * Handlers for docker hosts
 * @param {Socket} socket Socket.io instance
 * @returns {void}
 */
module.exports.dockerSocketHandler = (socket) => {
    socket.on("addDockerHost", async (dockerHost, dockerHostID, callback) => {
        try {
            checkLogin(socket);

            let dockerHostBean = await DockerHost.save(dockerHost, dockerHostID, socket.userID);
            await sendDockerHostList(socket);

            callback({
                ok: true,
                msg: "Saved.",
                msgi18n: true,
                id: dockerHostBean.id,
            });
        } catch (e) {
            callback({
                ok: false,
                msg: e.message,
            });
        }
    });

    socket.on("deleteDockerHost", async (dockerHostID, callback) => {
        try {
            checkLogin(socket);

            await DockerHost.delete(dockerHostID, socket.userID);
            await sendDockerHostList(socket);

            callback({
                ok: true,
                msg: "successDeleted",
                msgi18n: true,
            });
        } catch (e) {
            callback({
                ok: false,
                msg: e.message,
            });
        }
    });

    socket.on("testDockerHost", async (dockerHost, callback) => {
        try {
            checkLogin(socket);

            let amount = await DockerHost.testDockerHost(dockerHost);
            let msg;

            if (amount >= 1) {
                msg = "Connected Successfully. Amount of containers: " + amount;
            } else {
                msg = "Connected Successfully, but there are no containers?";
            }

            callback({
                ok: true,
                msg,
            });
        } catch (e) {
            log.error("docker", e);

            callback({
                ok: false,
                msg: e.message,
            });
        }
    });

    /**
     * Get list of Docker Swarm stacks from a docker host
     */
    socket.on("getDockerSwarmStacks", async (dockerHostId, callback) => {
        try {
            checkLogin(socket);

            const dockerHost = await R.findOne("docker_host", " id = ? AND user_id = ? ", [dockerHostId, socket.userID]);
            if (!dockerHost) {
                throw new Error("Docker host not found");
            }

            const options = buildDockerAxiosOptions(dockerHost);
            await applyDockerTlsOptions(dockerHost, options);

            // Get all services and extract unique stack names
            const response = await axios.request({
                ...options,
                url: "/services",
            });

            const stacks = new Set();
            for (const service of response.data || []) {
                const stackName = service.Spec?.Labels?.["com.docker.stack.namespace"];
                if (stackName) {
                    stacks.add(stackName);
                }
            }

            callback({
                ok: true,
                stacks: Array.from(stacks).sort(),
            });
        } catch (e) {
            log.error("docker", e);
            callback({
                ok: false,
                msg: e.message,
            });
        }
    });

    /**
     * Get services in a Docker Swarm stack
     */
    socket.on("getDockerSwarmStackServices", async (dockerHostId, stackName, callback) => {
        try {
            checkLogin(socket);

            const dockerHost = await R.findOne("docker_host", " id = ? AND user_id = ? ", [dockerHostId, socket.userID]);
            if (!dockerHost) {
                throw new Error("Docker host not found");
            }

            const options = buildDockerAxiosOptions(dockerHost);
            await applyDockerTlsOptions(dockerHost, options);

            const filters = {
                label: [`com.docker.stack.namespace=${stackName}`]
            };

            const response = await axios.request({
                ...options,
                url: `/services?filters=${encodeURIComponent(JSON.stringify(filters))}`,
            });

            const services = (response.data || []).map(service => ({
                id: service.ID,
                name: service.Spec?.Name || service.ID,
                replicas: service.Spec?.Mode?.Replicated?.Replicas || 1,
                image: service.Spec?.TaskTemplate?.ContainerSpec?.Image?.split("@")[0] || "unknown",
            }));

            callback({
                ok: true,
                services,
            });
        } catch (e) {
            log.error("docker", e);
            callback({
                ok: false,
                msg: e.message,
            });
        }
    });
};

/**
 * Build axios options for Docker API requests
 * @param {object} dockerHost Docker host bean
 * @returns {object} Axios request options
 */
function buildDockerAxiosOptions(dockerHost) {
    const options = {
        timeout: 10000,
        headers: {
            "Accept": "*/*",
        },
        httpsAgent: new https.Agent({
            maxCachedSessions: 0,
            rejectUnauthorized: true,
            secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
        }),
        httpAgent: new http.Agent({
            maxCachedSessions: 0,
        }),
    };

    if (dockerHost.docker_type === "socket") {
        options.socketPath = dockerHost.docker_daemon;
    } else if (dockerHost.docker_type === "tcp") {
        options.baseURL = DockerHost.patchDockerURL(dockerHost.docker_daemon);
    }

    return options;
}

/**
 * Apply TLS options for TCP connections
 * @param {object} dockerHost Docker host bean
 * @param {object} options Axios options to update
 * @returns {Promise<void>}
 */
async function applyDockerTlsOptions(dockerHost, options) {
    if (dockerHost.docker_type === "tcp") {
        options.httpsAgent = new https.Agent(
            await DockerHost.getHttpsAgentOptions(dockerHost.docker_type, options.baseURL)
        );
    }
}
