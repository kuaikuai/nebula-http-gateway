# nebula-http-gateway

Gateway to provide a http interface for the Nebula Graph service.

## Build

```bash
$ cd /path/to/nebula-http-gateway
$ make
```

## Run

```bash
$ ./nebula-httpd
```

## Required

- Go 1.13+
- [beego](https://beego.me/)

## Version
| Nebula Graph version | Http-gateway tag | 
|----------------------|---------------------------|
| 1.x                  | v1.0                      |
| 2.0.x & 2.5.x        | v2.0                      |
| 2.6.x                | v2.1.x                    |
| 3.0.x                | 2.2.x                     |
| 3.1.x                | 3.1.x                     |
| 3.2.x                | 3.2.x                     |

## User Guide

### API Definition

| Name       | Path               | Method |
|------------|--------------------|--------|
| connect    | /api/db/connect    | POST   |
| exec       | /api/db/exec       | POST   |
| disconnect | /api/db/disconnect | POST   |
| import     | /api/task/import   | POST   |
| action     | /api/task/import/action | POST |
| copy       | /api/task/copy     | POST   |
| sync-es    | /api/task/sync-es  | POST   |

#### Connect API ####

The request json body

```json
{
  "username": "user",
  "password": "password",
  "address": "192.168.8.26",
  "port": 9669
}
```

The description of the parameters is as follows:

| Field    | Description                                                                                                                 |
|----------|-----------------------------------------------------------------------------------------------------------------------------|
| username | Sets the username of your Nebula Graph account. Before enabling authentication, you can use any characters as the username. |
| password | Sets the password of your Nebula Graph account. Before enabling authentication, you can use any characters as the password. |
| address  | Sets the IP address of the graphd service.                                                                                  |
| port     | Sets the port number of the graphd service. The default port number is 9669.                                                |

```bash
$ curl -i -X POST \
    -d '{"username":"user","password":"password","address":"192.168.8.26","port":9669}' \
    http://127.0.0.1:8080/api/db/connect
```

response:

```
HTTP/1.1 200 OK
Content-Length: 100
Content-Type: application/json; charset=utf-8
Server: beegoServer:1.12.3
Set-Cookie: common-nsid=bec2e665ba62a13554b617d70de8b9b9; Path=/; HttpOnly
Set-Cookie: Secure=true; Path=/
Set-Cookie: SameSite=None; Path=/
Date: Fri, 02 Apr 2021 08:49:18 GMT

{
  "code": 0,
  "data": "5e18fa40-5343-422f-84e3-e7f9cad6b735",
  "message": "Login successfully"
}
```

Notice:

The response data nsid `5e18fa40-5343-422f-84e3-e7f9cad6b735` is encoded by HMAC-SH256 encryption algorithm, so it's not the same as what you get from a cookie.
If you connect to the graphd service successfully, remember to save the *NSID* locally, which is important for the *exec* api to execute nGQL.
If you restart the gateway server, all authenticated session will be lost, please be aware of this.

#### Exec API ####

The requested json body

```json
{
  "gql": "show spaces;"
}
```

**Cookie** is required in the request header to request `exec` api.


```bash
$ curl -X POST \
    -H "Cookie: SameSite=None; common-nsid=bec2e665ba62a13554b617d70de8b9b9" \
    -d '{"gql": "show spaces;"}' \
    http://127.0.0.1:8080/api/db/exec
```

response:

```json
{
  "code": 0,
  "data": {
    "headers": [
      "Name"
    ],
    "tables": [
      {
        "Name": "nba"
      }
    ],
    "timeCost": 4232
  },
  "message": ""
}
```

#### Disconnect API ####

```bash
$ curl -X POST -H "Cookie:common-nsid=bec2e665ba62a13554b617d70de8b9b9" http://127.0.0.1:8080/api/db/disconnect
```

response:

```json
{
  "code": 0,
  "data": null,
  "message": "Disconnect successfully"
}
```

#### Import API #### 

The requested json body

```json
{
  "configPath": "examples/v2/example.yaml"
}
```

The description of the parameters is as follows.

| Field      | Description                                                  |
| ---------- | ------------------------------------------------------------ |
| configPath | `configPath` is a relative path that under the `uploadspath` in `app.conf`. |
| configBody | `configBody` is the detail configuration with JSON format (instead of YAML format).|

If you choose to use `configPath`, you need to make sure that the config file has been uploaded to `uploadspath`.

```bash
$ curl -X POST -d '{"configPath": "./examples/v2/example.yaml","configBody": {}}' http://127.0.0.1:8080/api/task/import
```

If you choose to use `configBody`, you need to set the `configPath` value to `""` and set the `configBody` as JSON format.

response:

```json
{
    "code": 0,
    "data": [
        "1"
    ],
    "message": "Import task 1 submit successfully"
}
```

#### Action API ####

The requested json body

```json
{
  "taskID": "1",
  "taskAction": "actionQuery"
}
```

The description of the parameters is as follows.

| Field      | Description                                          |
| ---------- | ---------------------------------------------------- |
| taskID     | Set the task id to do task action                    |
| taskAction | The task action enums, include: actionQuery, actionQueryAll, actionStop, actionStopAll, etc. |

```bash
$ curl -X POST -d '{"taskID": "1", "taskAction": "actionQuery"}' http://127.0.0.1:8080/api/task/import/action
```

response:

```json
{
  "code": 0,
  "data": {
    "results": [
      {
        "taskID": "1",
        "taskStatus": "statusProcessing"
      }
    ],
    "msg": "Task is processing"
  },
  "message": "Processing a task action successfully"
}
```

#### Copy Space API ####

The Copy Space API copies all data from a source space to a destination space, including tags, edges, indexes, listeners, and full-text indexes.

The requested json body

```json
{
  "src_space": "nba",
  "dst_space": "nba_copy",
  "force": true,
  "partition_num": 0,
  "replica_factor": 0,
  "vid_type": "",
  "debug": false,
  "batch_size": 1000
}
```

The description of the parameters is as follows.

| Field         | Description                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| src_space     | The name of the source space to copy from.                                                               |
| dst_space     | The name of the destination space to copy to.                                                            |
| force         | If true, the destination space will be dropped first if it exists.                                       |
| partition_num | The number of partitions for the destination space. If 0, uses the source space's partition count.    |
| replica_factor| The replica factor for the destination space. If 0, uses the source space's replica factor.             |
| vid_type      | The VID type for the destination space (e.g., "INT64", "FIXED_STRING(8)"). If empty, uses source type. |
| debug         | If true, outputs debug nGQL statements to logs.                                                         |
| batch_size    | The batch size for scanning and inserting data. If 0, uses the configured default (copyBatchSize).      |

```bash
$ curl -X POST \
    -H "Cookie: SameSite=None; common-nsid=bec2e665ba62a13554b617d70de8b9b9" \
    -d '{"src_space": "nba", "dst_space": "nba_copy", "force": true}' \
    http://127.0.0.1:8080/api/task/copy
```

response:

```json
{
  "code": 0,
  "data": ["task_id_12345"],
  "message": "Copy task task_id_12345 submit successfully"
}
```

You can query the task status using the Action API:

```bash
$ curl -X POST -d '{"taskID": "task_id_12345", "taskAction": "actionQuery"}' http://127.0.0.1:8080/api/task/import/action
```

**Configuration:**

The default batch size can be configured in `conf/app.conf`:

```conf
copyBatchSize = 1000
```

#### Sync ES API ####

The Sync ES API synchronizes data from NebulaGraph to Elasticsearch full-text indexes. It scans vertices or edges using `scanVertices`/`scanEdges` and writes to the specified ES index.

**Prerequisites:**

1. Elasticsearch must be configured as a text search client in NebulaGraph
2. A full-text index must exist in NebulaGraph (created via `CREATE FULLTEXT INDEX`)
3. The ES index name must match the NebulaGraph full-text index name

The requested json body

```json
{
  "space": "nba",
  "es_index": "player_idx",
  "batch_size": 1000,
  "es_username": "elastic",
  "es_password": "password"
}
```

The description of the parameters is as follows.

| Field        | Description                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------- |
| space        | The NebulaGraph space name to sync from.                                                             |
| es_index     | The ES index name (must match a NebulaGraph full-text index name).                                 |
| batch_size   | The batch size for bulk写入 ES. If 0, uses default 1000.                                            |
| es_username  | Elasticsearch username for authentication (optional if ES has no auth).                             |
| es_password  | Elasticsearch password for authentication (optional if ES has no auth).                            |

```bash
$ curl -X POST \
    -H "Cookie: SameSite=None; common-nsid=bec2e665ba62a13554b617d70de8b9b9" \
    -d '{"space": "nba", "es_index": "player_idx"}' \
    http://127.0.0.1:8080/api/task/sync-es
```

response:

```json
{
  "code": 0,
  "data": {
    "task_id": "task_id_67890"
  },
  "message": "Sync task task_id_67890 submitted successfully"
}
```

**How it works:**

1. Queries `SHOW TEXT SEARCH CLIENTS` to discover ES cluster addresses
2. Queries `SHOW FULLTEXT INDEXES` to get the index field mappings
3. If the index is on a **Tag**: uses `scanVertices` to traverse and write to ES
4. If the index is on an **Edge**: uses `scanEdges` to traverse and write to ES
5. Filters properties to only include fields defined in the full-text index
6. Generates doc IDs using SHA256 hash (matching NebulaGraph's official format)

**Use cases:**

- Rebuild ES index after data loss
- One-time migration of NebulaGraph data to ES
- Periodic data synchronization

**Note:** This API performs one-way sync from NebulaGraph to ES. It does not keep ES in real-time sync.

