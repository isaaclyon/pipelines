const { SnowflakeDriver } = require('@cubejs-backend/snowflake-driver');

const DISABLE_SECONDARY_ROLES = 'USE SECONDARY ROLES NONE';

/**
 * Disable secondary roles on every dedicated reader session. This is defense
 * in depth; the reader identity must have no writer or restricted grants.
 * Initialization fails closed if the session cannot disable secondary roles.
 */
class SnowflakeReaderDriver extends SnowflakeDriver {
  /**
   * Runs for the first connection and for each reconnection. The pinned base
   * method only runs session ALTER statements before it returns, and no
   * application query uses the connection until this method resolves.
   */
  async initConnection() {
    const connection = await super.initConnection();
    try {
      await this.execute(connection, DISABLE_SECONDARY_ROLES, [], false);
      return connection;
    } catch (error) {
      this.connection = null;
      await new Promise((resolve) => connection.destroy(() => resolve(undefined)));
      throw error;
    }
  }

  /** Checks a fully initialized reader session, then closes it. */
  async testConnection() {
    const connection = await this.initConnection();
    await new Promise((resolve, reject) =>
      connection.destroy((error) => (error ? reject(error) : resolve(undefined))));
  }
}

module.exports = { SnowflakeReaderDriver, DISABLE_SECONDARY_ROLES };
