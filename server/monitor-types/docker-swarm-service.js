const { MonitorType } = require("./monitor-type");
const { UP, PENDING, DOWN, log } = require("../../src/util");
const { R } = require("redbean-node");
const axios = require("axios");
const https = require("https");
const http = require("http");
const crypto = require("crypto");
const dayjs = require("dayjs");
const { DockerHost } = require("../docker");

class DockerSwarmServiceMonitorType extends MonitorType {
    name = "docker-swarm-service";
    allowCustomStatus = true;

    /**
     * @inheritdoc
     */
    async check(monitor, heartbeat, server) {
        const startTime = dayjs().valueOf();

        const dockerHost = await R.load("docker_host", monitor.docker_host);

        if (!dockerHost) {
            throw new Error("Docker host not configured");
        }

        const options = this.buildAxiosOptions(monitor, dockerHost);

        // Apply TLS options for TCP connections
        await this.applyTlsOptions(dockerHost, options);

        // First, get the service by name or ID
        const service = await this.getService(monitor.docker_service, options);

        if (!service) {
            throw new Error(`Service '${monitor.docker_service}' not found`);
        }

        // Get service tasks to check their status
        const tasks = await this.getServiceTasks(service.ID, options);

        // Determine service health based on task status
        const healthResult = this.evaluateServiceHealth(service, tasks);

        heartbeat.ping = dayjs().valueOf() - startTime;

        if (healthResult.healthy) {
            heartbeat.status = UP;
            heartbeat.msg = healthResult.message;
        } else {
            // Check grace period for rolling updates
            const gracePeriod = monitor.docker_swarm_grace_period || 30;
            const isWithinGracePeriod = await this.isWithinGracePeriod(monitor.id, gracePeriod);

            if (isWithinGracePeriod) {
                heartbeat.status = PENDING;
                heartbeat.msg = `${healthResult.message} (within ${gracePeriod}s grace period)`;
            } else {
                heartbeat.status = DOWN;
                heartbeat.msg = healthResult.message;
            }
        }
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
     * Fetch HTTPS agent options for TCP connections
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
     * Get a Docker Swarm service by name or ID
     * @param {string} serviceName Service name or ID
     * @param {object} options Axios options
     * @returns {Promise<object|null>} Service object or null if not found
     */
    async getService(serviceName, options) {
        // First try to get the service directly by ID or name
        try {
            const response = await axios.request({
                ...options,
                url: `/services/${encodeURIComponent(serviceName)}`,
            });
            return response.data;
        } catch (error) {
            // If not found by ID, try to filter by name
            if (error.response && error.response.status === 404) {
                const filterResponse = await axios.request({
                    ...options,
                    url: `/services?filters=${encodeURIComponent(JSON.stringify({ name: [serviceName] }))}`,
                });

                if (filterResponse.data && filterResponse.data.length > 0) {
                    return filterResponse.data[0];
                }
                return null;
            }
            throw error;
        }
    }

    /**
     * Get tasks for a specific service
     * @param {string} serviceId Service ID
     * @param {object} options Axios options
     * @returns {Promise<Array>} Array of task objects
     */
    async getServiceTasks(serviceId, options) {
        const response = await axios.request({
            ...options,
            url: `/tasks?filters=${encodeURIComponent(JSON.stringify({ service: [serviceId] }))}`,
        });
        return response.data || [];
    }

    /**
     * Evaluate the health of a Docker Swarm service based on its tasks
     * @param {object} service Service object
     * @param {Array} tasks Array of task objects
     * @returns {{healthy: boolean, message: string}} Health evaluation result
     */
    evaluateServiceHealth(service, tasks) {
        // Get desired replica count from service spec
        let desiredReplicas = 1;
        if (service.Spec && service.Spec.Mode) {
            if (service.Spec.Mode.Replicated) {
                desiredReplicas = service.Spec.Mode.Replicated.Replicas || 1;
            } else if (service.Spec.Mode.Global) {
                // For global services, count running tasks as "desired"
                // Global services run one task per node
                desiredReplicas = tasks.filter(t => t.DesiredState === "running").length || 1;
            }
        }

        // Count running tasks (only count the most recent task per slot for replicated services)
        const runningTasks = this.countRunningTasks(tasks);

        const serviceName = service.Spec?.Name || service.ID;

        if (runningTasks >= desiredReplicas) {
            return {
                healthy: true,
                message: `Service '${serviceName}' is healthy: ${runningTasks}/${desiredReplicas} tasks running`,
            };
        } else if (runningTasks === 0) {
            return {
                healthy: false,
                message: `Service '${serviceName}' has no running tasks (0/${desiredReplicas})`,
            };
        } else {
            return {
                healthy: false,
                message: `Service '${serviceName}' is degraded: ${runningTasks}/${desiredReplicas} tasks running`,
            };
        }
    }

    /**
     * Count running tasks, considering only the most recent task per slot
     * @param {Array} tasks Array of task objects
     * @returns {number} Number of running tasks
     */
    countRunningTasks(tasks) {
        // Group tasks by slot (for replicated services) or node (for global services)
        const tasksBySlot = new Map();

        for (const task of tasks) {
            const slot = task.Slot || task.NodeID || "default";
            const existing = tasksBySlot.get(slot);

            // Keep the most recent task for each slot
            if (!existing || new Date(task.CreatedAt) > new Date(existing.CreatedAt)) {
                tasksBySlot.set(slot, task);
            }
        }

        // Count tasks that are in running state
        let runningCount = 0;
        for (const task of tasksBySlot.values()) {
            if (task.Status && task.Status.State === "running") {
                runningCount++;
            }
        }

        return runningCount;
    }

    /**
     * Check if we're within the grace period since the service first became degraded
     * @param {number} monitorId Monitor ID
     * @param {number} gracePeriodSeconds Grace period in seconds
     * @returns {Promise<boolean>} True if within grace period
     */
    async isWithinGracePeriod(monitorId, gracePeriodSeconds) {
        // Find the last UP heartbeat
        const lastUpBeat = await R.findOne(
            "heartbeat",
            " monitor_id = ? AND status = ? ORDER BY time DESC",
            [monitorId, UP]
        );

        if (!lastUpBeat) {
            // No previous UP state, so we're not in a rolling update scenario
            // but could be initial deployment - allow grace period from first beat
            const firstBeat = await R.findOne(
                "heartbeat",
                " monitor_id = ? ORDER BY time ASC",
                [monitorId]
            );

            if (!firstBeat) {
                // No heartbeats at all, this is the first check - allow grace period
                return true;
            }

            const firstBeatTime = dayjs(firstBeat.time);
            const now = dayjs();
            const secondsSinceFirstBeat = now.diff(firstBeatTime, "second");

            return secondsSinceFirstBeat <= gracePeriodSeconds;
        }

        const lastUpTime = dayjs(lastUpBeat.time);
        const now = dayjs();
        const secondsSinceLastUp = now.diff(lastUpTime, "second");

        return secondsSinceLastUp <= gracePeriodSeconds;
    }
}

module.exports = {
    DockerSwarmServiceMonitorType,
};
