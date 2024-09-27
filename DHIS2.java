// camel-k: language=java

//DEPS org.apache.camel:camel-debezium-common:4.6.0
//DEPS org.apache.camel:camel-core:4.6.0

import io.debezium.data.Envelope;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.debezium.DebeziumConstants;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.connect.data.Struct;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;



public class DHIS2 extends RouteBuilder {
    private static final String EVENT_TYPE_TEI = ".trackedentityinstance";
    private static final String EVENT_TYPE_DV = ".datavalue";

    @Override
    public void configure() throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Struct.class, new StructSerializer());
        objectMapper.registerModule(module);

        final Predicate isCreateOrUpdateEvent = header(DebeziumConstants.HEADER_OPERATION)
                .in(constant(Envelope.Operation.READ.code()),
                        constant(Envelope.Operation.CREATE.code()),
                        constant(Envelope.Operation.UPDATE.code())
                );
        final Predicate isTrackedEntityInstanceEvent = header(DebeziumConstants.HEADER_IDENTIFIER).endsWith(EVENT_TYPE_TEI);

        final Predicate isDataValueEvent = header(DebeziumConstants.HEADER_IDENTIFIER).endsWith(EVENT_TYPE_DV);

        from("debezium-postgres:dhis40" +
                "?offsetStorageFileName=/Users/carapai/offset-file-1.dat" +
                "&databaseHostname=localhost" +
                "&databaseUser=carapai" +
                "&databasePassword=Baby77@Baby771" +
                "&databaseDbname=dhis40" +
                "&topicPrefix=dhis40" +
                "&pluginName=pgoutput" +
                "&tableIncludeList=public.datavalue,public.trackedentityinstance" +
                "&schemaIncludeList=public")
                .routeId(DHIS2.class.getName() + ".DatabaseReader")
                .choice()
                .when(isTrackedEntityInstanceEvent)
                .filter(isCreateOrUpdateEvent)
                .process(exchange -> {
                    Struct struct = exchange.getIn().getBody(Struct.class);
                    String url = "http://localhost:3001/layer?instance=" + struct.getString("uid");
                    exchange.getIn().setHeader("CamelHttpUri", url);
                })
                .to("http://dummy")
                .log("Sent instance fo ${body}")
                .endChoice()
                .when(isDataValueEvent)
                .filter(isCreateOrUpdateEvent)
                .log("Found data value ${body}")
                .endChoice()
                .otherwise()
                .log("Unknown type ${headers[" + DebeziumConstants.HEADER_IDENTIFIER + "]}")
                .endParent();
    }
}



class StructSerializer extends StdSerializer<Struct> {

    public StructSerializer() {
        this(null);
    }

    public StructSerializer(Class<Struct> t) {
        super(t);
    }

    @Override
    public void serialize(Struct value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        value.schema().fields().forEach(field -> {
            try {
                gen.writeObjectField(field.name(), value.get(field));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        gen.writeEndObject();
    }
}