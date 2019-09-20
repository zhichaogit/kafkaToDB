PARTITION=$1
BROKER=$2
DBIP=$3
TOPIC=$4
FORMAT=$5
DESTSCHEMA=$6
TABLE1=$7
TABLE2=$8

CONFILE=$9

echo "{
	\"database\": {
		\"batchSize\": 5000,
		\"DBIP\": \"$DBIP\",
		\"DBPort\": \"23400\",
		\"DBType\": \"EsgynDB\",
		\"DBDriver\": \"org.trafodion.jdbc.t4.T4Driver\",
		\"DBUser\": \"db__root\",
		\"DBPW\": \"zz\",
		\"DBTenant\": null,
		\"defSchema\": null,
		\"defTable\": null,
		\"keepalive\": false
	},
	\"kafka\": {
		\"broker\": \"localhost:9092\",
		\"fetchBytes\": 104857600,
		\"fetchSize\": 10000,
		\"mode\": \"start\",
		\"topics\": [{
		 	  \"topic\":\"${TOPIC}\",
			  \"partition\":${PARTITION},
			  \"group\": \"0\"
                }],
		\"kafkaUser\": null,
		\"kafkaPW\": null,
		\"key\": \"org.apache.kafka.common.serialization.StringDeserializer\",
		\"value\": \"org.apache.kafka.common.serialization.StringDeserializer\",
		\"streamTO\": 5,
		\"waitTO\": 2,
		\"zkTO\": 10,
		\"hbTO\": 10,
		\"seTO\": 30,
		\"reqTO\": 305,
		\"zookeeper\": null
	},
	\"kafkaCDC\": {
		\"consumers\" : 4,
		\"bigEndian\": false,
		\"delimiter\": \",\",
		\"dumpBinary\": true,
		\"encoding\": \"UTF8\",
		\"format\": \"${FORMAT}\",
		\"interval\": 2,
		\"skip\": false,
		\"loaders\": 4,
		\"loadDir\": \"load\",
		\"kafkaDir\": \"kafka\",
		\"showConsumers\": true,
		\"showLoaders\": true,
		\"showTasks\": true,
		\"showTables\": true,
		\"showSpeed\": true
	},
	\"mappings\": [{
		\"srcSchemaName\": \"${DESTSCHEMA}SRC\",
		\"srcTableName\": \"${TABLE1}SRC\",
		\"schemaName\": \"${DESTSCHEMA}\",
		\"tableName\": \"${TABLE1}\",
		\"columns\": [{
			\"srcColumnName\": \"\",
			\"columnName\": \"t001_c1\"
		}, {
			\"srcColumnName\": \"\",
			\"columnName\": \"t001_c2\"
		}]
	}, {
		\"srcSchemaName\": \"${DESTSCHEMA}SRC\",
		\"srcTableName\": \"${TABLE2}SRC\",
		\"schemaName\": \"${DESTSCHEMA}\",
		\"tableName\": \"${TABLE2}\"
	}]
}" > $CONFILE
