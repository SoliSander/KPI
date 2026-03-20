const fs = require("fs");
const { parse } = require("csv-parse/sync");
const minimist = require("minimist");
const { loadFactTable } = require('./loadFact');

// ---------------------------
// Parse command-line arguments
// ---------------------------
const args = minimist(process.argv.slice(1), {
    alias: {
        i: "input",
    }
});

if (!args.input) {
    console.error("Usage: node initial_load_general.js --input=source.csv");
    process.exit(1);
}

const inputFile = args.input;
console.log("Reading:", inputFile);

// ---------------------------
// Read + parse CSV
// ---------------------------
const input = fs.readFileSync(inputFile, "utf8");

let rows = parse(input, {
    delimiter: ",",
    relax_column_count: true,
    from_line: 2
});

loadFactTable(rows)
.then(() => {
    console.log(`Fact table loaded! (Rows: ${rows.length})`);
    process.exit(0);
})
.catch((err) => {
    console.error(err);
    process.exit(1);
});