function proc(e) {
    e.getIn().setBody("Hello Camel K!");
}

from(
    "debezium-postgres:dhis40?offsetStorageFileName=/Users/carapai/offset-file-1.dat&databaseHostname=localhost&databaseUser=carapai&databasePassword=Baby77@Baby771&databaseDbname=dhis40&topicPrefix=dhis40&pluginName=pgoutput&tableIncludeList=public.datavalue,public.programstageinstance,public.programinstance,public.trackedentityinstance,public.trackedentityattributevalue&schemaIncludeList=public"
)
    .log("Event received from Debezium : ${body}")
    .log("    with this identifier ${headers.CamelDebeziumIdentifier}")
    .log(
        "    with these source metadata ${headers.CamelDebeziumSourceMetadata}"
    )
    .log(
        "    the event occurred upon this operation '${headers.CamelDebeziumSourceOperation}'"
    )
    .log(
        "    on this database '${headers.CamelDebeziumSourceMetadata[db]}' and this table '${headers.CamelDebeziumSourceMetadata[table]}'"
    )
    .log("    with the key ${headers.CamelDebeziumKey}")
    .log("    the previous value is ${headers.CamelDebeziumBefore}");
