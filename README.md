# DataShunt
An SDB Data wrangler.

More so it will build a schema in a Postgres DB from a given SDB file as well as import the data into it.

The format being having the tables with the hex names of the hashes and then views that map names to those values.

## To Run

- Clone the repo and build or download a binary (if there is one)
- Adjust the Config.json in the Workshop folder to suit if needed. (change the sdb path or drop the sdb beside the config)
- Run the DataSunt.exe with the args ''--conn-str "Your database connection string" -dir "path to your Config.json dir"''
- That should be it :>

## DB Structure

The result of the import should be

- 7 new types added to the schema
- Tables created for the ones in the SDB (with names in the format H_HexId, eg. H_35D88CC2)
- Views mapping the hash table names to strings
- The data imported
- A TableMappings.json file in the set dir, this file contains info for mapping the tables and types back.
