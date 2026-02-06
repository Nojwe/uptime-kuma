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

    /**
     * Sync Docker Swarm stack services - creates monitors for new services
     */
    socket.on("syncDockerSwarmStackServices", async (monitorId, callback) => {
        try {
            checkLogin(socket);

            const result = await syncStackServices(monitorId, socket.userID);

            // Start the newly created monitors
            if (result.newMonitorIds && result.newMonitorIds.length > 0) {
                const { UptimeKumaServer } = require("../uptime-kuma-server");
                const server = UptimeKumaServer.getInstance();

                for (const newMonitorId of result.newMonitorIds) {
                    const monitor = await R.findOne("monitor", " id = ? ", [newMonitorId]);
                    if (monitor && monitor.active) {
                        server.monitorList[monitor.id] = monitor;
                        await monitor.start(server.io);
                    }
                }
            }

            callback({
                ok: true,
                msg: result.message,
                newServices: result.newServices,
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

/**
 * Sync services for a Docker Swarm stack group monitor
 * Creates child monitors for any new services discovered
 * @param {number} monitorId The stack group monitor ID
 * @param {number} userId The user ID
 * @returns {Promise<{message: string, newServices: string[]}>}
 */
async function syncStackServices(monitorId, userId) {
    const Monitor = require("../model/monitor");

    // Get the stack monitor
    const stackMonitor = await R.findOne("monitor", " id = ? AND user_id = ? ", [monitorId, userId]);
    if (!stackMonitor) {
        throw new Error("Monitor not found");
    }

    if (!stackMonitor.docker_stack) {
        throw new Error("Monitor is not a Docker Swarm stack");
    }

    // Get the docker host
    const dockerHost = await R.findOne("docker_host", " id = ? AND user_id = ? ", [stackMonitor.docker_host, userId]);
    if (!dockerHost) {
        throw new Error("Docker host not found");
    }

    // Query Docker for services in this stack
    const options = buildDockerAxiosOptions(dockerHost);
    await applyDockerTlsOptions(dockerHost, options);

    const filters = {
        label: [`com.docker.stack.namespace=${stackMonitor.docker_stack}`]
    };

    const response = await axios.request({
        ...options,
        url: `/services?filters=${encodeURIComponent(JSON.stringify(filters))}`,
    });

    const services = response.data || [];

    // Get existing child monitors
    const existingChildren = await Monitor.getChildren(monitorId);
    const existingByService = new Map();
    for (const child of existingChildren) {
        if (child.docker_service) {
            existingByService.set(child.docker_service, child);
        }
    }

    // Create monitors for new services
    const newServices = [];
    const newMonitorIds = [];
    for (const service of services) {
        const serviceName = service.Spec?.Name || service.ID;

        if (!existingByService.has(serviceName)) {
            // Create new child monitor
            const bean = R.dispense("monitor");
            const displayName = serviceName.replace(`${stackMonitor.docker_stack}_`, "");

            bean.name = displayName;
            bean.type = "docker-swarm-service";
            bean.user_id = userId;
            bean.parent = monitorId;
            bean.docker_host = stackMonitor.docker_host;
            bean.docker_service = serviceName;
            bean.docker_swarm_grace_period = stackMonitor.docker_swarm_grace_period || 30;
            bean.interval = stackMonitor.interval || 60;
            bean.retryInterval = stackMonitor.retryInterval || stackMonitor.interval || 60;
            bean.maxretries = stackMonitor.maxretries || 0;
            bean.active = true;
            bean.accepted_statuscodes_json = JSON.stringify(["200-299"]);

            await R.store(bean);
            newServices.push(displayName);
            newMonitorIds.push(bean.id);

            log.info("docker-swarm-stack", `Created monitor for service '${serviceName}' in stack '${stackMonitor.docker_stack}'`);
        }
    }

    if (newServices.length === 0) {
        return {
            message: "No new services found",
            newServices: [],
            newMonitorIds: [],
        };
    }

    return {
        message: `Created ${newServices.length} new service monitor(s)`,
        newServices,
        newMonitorIds,
    };
}

module.exports.syncStackServices = syncStackServices;
