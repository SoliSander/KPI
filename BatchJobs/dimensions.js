function normalize(value) {
    if (value === undefined || value === null) return null;

    const v = String(value).trim();
    return v === "" ? null : v;
}

async function getDimensionId(client, table, column, value) {
  if (value === undefined || value === null) return null;

  const clean = String(value).trim();
  if (clean === '') return null;

  const sql = `
    INSERT INTO ${table} (${column})
    VALUES ($1)
    ON CONFLICT (LOWER(${column}))
    WHERE ${column} IS NOT NULL
    DO UPDATE SET ${column} = ${table}.${column}
    RETURNING id
  `;

  const res = await client.query(sql, [clean]);

  if (res.rows.length === 0) {
    throw new Error(
      `Dimension lookup failed: ${table}.${column} = "${clean}"`
    );
  }

  return res.rows[0].id;
}

module.exports = { getDimensionId }