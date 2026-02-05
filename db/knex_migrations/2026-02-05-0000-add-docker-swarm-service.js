/**
 * @param {import("knex").Knex} knex The Knex.js instance for database interaction.
 * @returns {Promise<void>}
 */
exports.up = async (knex) => {
    await knex.schema.alterTable("monitor", (table) => {
        table.string("docker_service", 255);
        table.integer("docker_swarm_grace_period").defaultTo(30);
    });
};

/**
 * @param {import("knex").Knex} knex The Knex.js instance for database interaction.
 * @returns {Promise<void>}
 */
exports.down = async (knex) => {
    await knex.schema.alterTable("monitor", (table) => {
        table.dropColumn("docker_service");
        table.dropColumn("docker_swarm_grace_period");
    });
};
