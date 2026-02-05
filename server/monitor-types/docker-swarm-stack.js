const { MonitorType } = require("./monitor-type");
const { UP, PENDING, DOWN, log } = require("../../src/util");
const { R } = require("redbean-node");
const axios = require("axios");
const https = require("https");
const http = require("http");
const crypto = require("crypto");
const { DockerHost } = require("../docker");
const Monitor = require("../model/monitor");

class DockerSwarmStackMonitorType extends MonitorType {
    name = "docker-swarm-stack";

    /**
     * @inheritdoc
     */
    async check(monitor, heartbeat, server) {
        const dockerHost = await R.load("docker_host", monitor.docker_host);

        if (!dockerHost) {
            throw new Error("Docker host not configured");
        }

        const options = this.buildAxiosOptions(monitor, dockerHost);
        await this.applyTlsOptions(dockerHost, options);

        // Get all services in the stack (may be empty if stack was removed)
        const services = await this.getStackServices(monitor.docker_stack, options);

        // Only create new monitors for newly discovered services
        // Don't remove monitors for missing services - they'll show as DOWN
        if (services.length > 0) {
            await this.syncChildMonitors(monitor, services, server);
        }

        // Now aggregate status like GroupMonitorType
        const children = await Monitor.getChildren(monitor.id);

        if (children.length === 0) {
            heartbeat.status = PENDING;
            heartbeat.msg = `Stack '${monitor.docker_stack}' - syncing services...`;
            return;
        }

        let worstStatus = UP;
        const downChildren = [];
        const pendingChildren = [];

        for (const child of children) {
            if (!child.active) {
                continue;
            }

            const label = child.name || `#${child.id}`;
            const lastBeat = await Monitor.getPreviousHeartbeat(child.id);

            if (!lastBeat) {
                if (worstStatus === UP) {
                    worstStatus = PENDING;
                }
                pendingChildren.push(label);
                continue;
            }

            if (lastBeat.status === DOWN) {
                worstStatus = DOWN;
                downChildren.push(label);
            } else if (lastBeat.status === PENDING) {
                if (worstStatus !== DOWN) {
                    worstStatus = PENDING;
                }
                pendingChildren.push(label);
            }
        }

        if (worstStatus === UP) {
            heartbeat.status = UP;
            heartbeat.msg = `Stack '${monitor.docker_stack}' - all ${children.length} services healthy`;
            return;
        }

        if (worstStatus === PENDING) {
            heartbeat.status = PENDING;
            heartbeat.msg = `Stack '${monitor.docker_stack}' - pending: ${pendingChildren.join(", ")}`;
            return;
        }

        heartbeat.status = DOWN;
        let message = `Stack '${monitor.docker_stack}' - down: ${downChildren.join(", ")}`;
        if (pendingChildren.length > 0) {
            message += `; pending: ${pendingChildren.join(", ")}`;
        }
        throw new Error(message);
    }

    /**
     * Build axios options for Docker API requests
     * @param {object} monitor Monitor configuration
     * @param {object} dockerHost Docker host configuration
     * @returns {object} Axios request options
     */
    buildAxiosOptions(monitor, dockerHost) {
        const options = {
            timeout: monitor.interval * 1000 * 0.8,
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

        if (dockerHost._dockerType === "socket") {
            options.socketPath = dockerHost._dockerDaemon;
        } else if (dockerHost._dockerType === "tcp") {
            options.baseURL = DockerHost.patchDockerURL(dockerHost._dockerDaemon);
        }

        return options;
    }

    /**
     * Apply TLS options for TCP connections
     * @param {object} dockerHost Docker host configuration
     * @param {object} options Axios options to update
     * @returns {Promise<void>}
     */
    async applyTlsOptions(dockerHost, options) {
        if (dockerHost._dockerType === "tcp") {
            options.httpsAgent = new https.Agent(
                await DockerHost.getHttpsAgentOptions(dockerHost._dockerType, options.baseURL)
            );
        }
    }

    /**
     * Get all services belonging to a Docker Swarm stack
     * @param {string} stackName Stack name
     * @param {object} options Axios options
     * @returns {Promise<Array>} Array of service objects
     */
    async getStackServices(stackName, options) {
        // Docker Swarm stacks use the label "com.docker.stack.namespace" to identify services
        const filters = {
            label: [`com.docker.stack.namespace=${stackName}`]
        };

        const response = await axios.request({
            ...options,
            url: `/services?filters=${encodeURIComponent(JSON.stringify(filters))}`,
        });

        return response.data || [];
    }

    /**
     * Sync child monitors with discovered services
     * Only creates new monitors for new services - does NOT remove monitors
     * when services disappear (they will show as DOWN instead)
     * @param {object} monitor Parent stack monitor
     * @param {Array} services Discovered services from Docker
     * @param {object} server UptimeKumaServer instance
     * @returns {Promise<void>}
     */
    async syncChildMonitors(monitor, services, server) {
        const existingChildren = await Monitor.getChildren(monitor.id);

        // Map existing children by their docker_service name
        const existingByService = new Map();
        for (const child of existingChildren) {
            if (child.docker_service) {
                existingByService.set(child.docker_service, child);
            }
        }

        for (const service of services) {
            const serviceName = service.Spec?.Name || service.ID;

            // Check if child monitor already exists
            if (!existingByService.has(serviceName)) {
                // Create new child monitor for this service
                await this.createChildServiceMonitor(monitor, serviceName, service);
                log.info("docker-swarm-stack", `Created monitor for service '${serviceName}' in stack '${monitor.docker_stack}'`);
            }
        }

        // Note: We intentionally do NOT remove monitors for services that no longer exist
        // They will show as DOWN, which is the expected behavior when a stack is removed
    }

    /**
     * Create a child service monitor
     * @param {object} parentMonitor Parent stack monitor
     * @param {string} serviceName Service name
     * @param {object} service Service object from Docker API
     * @returns {Promise<void>}
     */
    async createChildServiceMonitor(parentMonitor, serviceName, service) {
        const bean = R.dispense("monitor");

        // Extract just the service name without stack prefix for display
        const displayName = serviceName.replace(`${parentMonitor.docker_stack}_`, "");

        bean.name = displayName;
        bean.type = "docker-swarm-service";
        bean.user_id = parentMonitor.user_id;
        bean.parent = parentMonitor.id;
        bean.docker_host = parentMonitor.docker_host;
        bean.docker_service = serviceName;
        bean.docker_swarm_grace_period = parentMonitor.docker_swarm_grace_period || 30;
        bean.interval = parentMonitor.interval || 60;
        bean.retryInterval = parentMonitor.retryInterval || parentMonitor.interval || 60;
        bean.maxretries = parentMonitor.maxretries || 0;
        bean.active = true;
        bean.accepted_statuscodes_json = JSON.stringify(["200-299"]);

        await R.store(bean);

        // Start the monitor
        const { startMonitor } = require("../util-server");
        await startMonitor(parentMonitor.user_id, bean.id);
    }
}

module.exports = {
    DockerSwarmStackMonitorType,
};
