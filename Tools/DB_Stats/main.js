const { Client } = require('pg');

async function getDbStats() {
  const client = new Client({
    user: "kpi",
    password: "kpiadmin",
    host: "localhost",
    port: 5432,
    database: "kpi"
  });

  await client.connect();

  const result = await client.query("SELECT get_database_table_sizes('kpi') AS stats;");

  console.log("=== Database Stats ===");
  console.log(result.rows[0].stats);

  await client.end();
}

getDbStats();