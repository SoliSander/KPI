const pool = require('./db');
const { getDimensionId } = require('./dimensions');

function normalizeNumber(value) {
    if (value === undefined || value === null) return null;

    const v = String(value).trim();
    if(v === '') return null;

    const n = Number(v);
    return Number.isNaN(n) ? null : n;
}

async function loadFactTable(rows) {
    const client = await pool.connect();
    let [datePart, timePart] = [];
    let [day, month, year] = [];
    let [hour, minute] = [];

    try {
        await client.query("BEGIN");

        for(const row of rows) {

            const issueKey = row[0];
            const issueId = row[1];
            const status = row[2];
            const created = row[3];
            const updated = row[4];
            const creator = row[5];
            const originalEstimate = row[6];
            const priority = row[7];
            const remainingEstimate = row[8];
            const timeSpent = row[9];
            const assignee = row[10];

            const issueKeyId = await getDimensionId(client, 'dimissuekey', "issuekey", issueKey);
            const statusId = await getDimensionId(client, 'dimstatus', "status", status);
            [datePart, timePart] = created.split(" ");
            [day, month, year] = datePart.split("/");
            [hour, minute] = timePart.split(":")
            const createdDate = `${year}-${month}-${day}`;
            const createdTime = `${hour}:${minute}:00`;
            [datePart, timePart] = updated.split(" ");
            [day, month, year] = datePart.split("/");
            [hour, minute] = timePart.split(":")
            const updatedDate = `${year}-${month}-${day}`;
            const updatedTime = `${hour}:${minute}:00`;
            const creatorId = await getDimensionId(client, 'dimcreator', "creator", creator);
            const calcOriginalEstimate = normalizeNumber(originalEstimate);
            const priorityId = await getDimensionId(client, 'dimpriority', "priority", priority);
            const calcRemainingEstimate = normalizeNumber(remainingEstimate);
            const calcTimeSpent = normalizeNumber(timeSpent);
            const assigneeId = await getDimensionId(client, 'dimassignee', "assignee", assignee);

            await client.query(
                `
                INSERT INTO facttable(
	            issueid, createddate, createdtime, updateddate, updatedtime, originalestimate, remainingestimate, issuekey, status, creator, priority, assignee, timespent)
	            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
                `,
                [issueId, createdDate, createdTime, updatedDate, updatedTime, calcOriginalEstimate, calcRemainingEstimate, issueKeyId, statusId, creatorId, priorityId, assigneeId, calcTimeSpent]
            );
        }
        await client.query("COMMIT");
    } catch (err) {
        await client.query("ROLLBACK");
        console.log(err);
    } finally {
        client.release();
    }
}

module.exports = { loadFactTable }