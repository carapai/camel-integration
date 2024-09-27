// camel-k: language=java
//DEPS org.apache.camel:camel-debezium-common:4.6.0
//DEPS org.apache.camel:camel-core:4.6.0
//DEPS org.apache.camel:camel-quartz:4.6.0
//DEPS org.apache.camel:camel-rest:4.6.0
//DEPS org.apache.camel:camel-http:4.6.0

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.quartz.QuartzComponent;
import org.apache.camel.model.rest.RestBindingMode;
import org.quartz.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DataImportWizard extends RouteBuilder {
    private static ScheduleStatus getScheduleStatus(Trigger trigger, String scheduleId) {
        Date nextFireTime = trigger.getNextFireTime();
        Date lastFireTime = trigger.getPreviousFireTime();

        // Prepare JSON response
        ScheduleStatus status = new ScheduleStatus();
        status.setScheduleId(scheduleId);
        status.setActive(true);
        status.setNextRunTime(nextFireTime != null ? nextFireTime.toString() : "N/A");
        status.setLastRunTime(lastFireTime != null ? lastFireTime.toString() : "N/A");
        return status;
    }

    private static ScheduleStatus getStatus(Trigger trigger, String scheduleId) {
        Date nextFireTime = trigger.getNextFireTime();
        Date lastFireTime = trigger.getPreviousFireTime();

        // Prepare JSON response
        ScheduleStatus status = new ScheduleStatus();
        status.setScheduleId(scheduleId);
        status.setActive(true);
        status.setNextRunTime(nextFireTime != null ? nextFireTime.toString() : "N/A");
        status.setLastRunTime(lastFireTime != null ? lastFireTime.toString() : "N/A");
        return status;
    }

    @Override
    public void configure() throws Exception {
        QuartzComponent quartz = getContext().getComponent("quartz", QuartzComponent.class);
        restConfiguration().component("netty-http")
                .host("0.0.0.0")
                .port(8081)
                .bindingMode(RestBindingMode.auto)
                .enableCORS(true)
                .corsHeaderProperty("Access-Control-Allow-Origin", "*")
                .corsHeaderProperty("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                .corsHeaderProperty("Access-Control-Allow-Headers", "Content-Type, Authorization");
        ;

        rest("/schedules")
                .post("/start")
                .consumes("application/json")
                .to("direct:startScheduler")
                .post("/resume")
                .consumes("application/json")
                .to("direct:resumeScheduler")
                .post("/stop")
                .consumes("application/json")
                .to("direct:stopScheduler")
                .get("/{scheduleId}/status")
                .produces("application/json")
                .to("direct:getScheduleStatus");


        // Route to handle starting a schedule
        from("direct:startScheduler")
                .log("Received request to start scheduler: ${body}")
                .process(exchange -> {
                    var body = exchange.getIn().getBody(Map.class);
                    String scheduleId = (String) body.get("scheduleId");
                    String cronExpression = (String) body.get("cronExpression");

                    Map<String, Object> currentData = new HashMap<>();
                    currentData.put("scheduleId", scheduleId);

                    getContext().addRoutes(new RouteBuilder() {
                        @Override
                        public void configure() {
                            fromF("quartz://%s/%s?cron=%s", scheduleId, scheduleId, cronExpression)
                                    .routeId(scheduleId)
                                    .setBody().constant(currentData)
                                    .marshal().json()
                                    .to("direct:postToApi")
                                    .log("Scheduled task executed for schedule: " + scheduleId);
                        }
                    });

                    exchange.getMessage().setBody("Scheduler started for schedule: " + scheduleId);
                }).onException(Exception.class)
                .log("Error encountered: ${exception.message}")
                .handled(true);
        ;

        // Route to handle resuming a schedule
        from("direct:resumeScheduler")
                .log("Received request to resume scheduler: ${body}")
                .process(exchange -> {
                    var body = exchange.getIn().getBody(Map.class);
                    String scheduleId = (String) body.get("scheduleId");
                    Scheduler scheduler = quartz.getScheduler();
                    if (scheduler != null) {
                        try {
                            scheduler.resumeJob(new JobKey(scheduleId, scheduleId));
                            exchange.getMessage().setBody("Scheduler resumed for schedule: " + scheduleId);
                        } catch (SchedulerException e) {
                            exchange.getMessage().setBody("Error resuming scheduler for schedule: " + scheduleId + " - " + e.getMessage());
                        }
                    }
                });


        // Route to handle stopping a schedule
        from("direct:stopScheduler")
                .log("Received request to stop scheduler: ${body}")
                .process(exchange -> {
                    var body = exchange.getIn().getBody(Map.class);
                    String scheduleId = (String) body.get("scheduleId");
                    Scheduler scheduler = quartz.getScheduler();
                    if (scheduler != null) {
                        try {
                            scheduler.deleteJob(new JobKey(scheduleId, scheduleId));
                            getContext().getRouteController().stopRoute(scheduleId);
                            getContext().removeRoute(scheduleId);
                            exchange.getMessage().setBody("Scheduler stopped for schedule: " + scheduleId);
                        } catch (SchedulerException e) {
                            exchange.getMessage().setBody("Error stopping scheduler for schedule: " + scheduleId + " - " + e.getMessage());
                        }
                    }
                });

        // Route to post to an API
        from("direct:postToApi")
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .log("Received request to start scheduler: ${body}")
                .to("http://localhost:3003")
                .log("Posted to API successfully");


        from("direct:getScheduleStatus")
                .log("Received request to get schedule status: ${body}")
                .process(exchange -> {
                    String scheduleId = exchange.getIn().getHeader("scheduleId", String.class);
                    Scheduler scheduler = quartz.getScheduler();
                    if (scheduler != null) {
                        JobKey jobKey = new JobKey(scheduleId, scheduleId);
                        boolean isScheduled = scheduler.checkExists(jobKey);
                        if (isScheduled) {
                            // Get the next and last run times
                            TriggerKey triggerKey = TriggerKey.triggerKey(scheduleId, scheduleId);
                            Trigger trigger = scheduler.getTrigger(triggerKey);
                            ScheduleStatus status = getStatus(trigger, scheduleId);

                            // Serialize status object to JSON
                            String jsonResponse = getContext().getTypeConverter().convertTo(String.class, status);

                            // Prepare SSE event
                            String sseEvent = "data: " + jsonResponse.replace("\n", "\ndata: ") + "\n\n";

                            // Set SSE event as response
                            exchange.getMessage().setBody(sseEvent);
                            exchange.getMessage().setHeader("Content-Type", "text/event-stream");
                        } else {
                            exchange.getMessage().setBody("{\"error\": \"Schedule " + scheduleId + " is not active\"}");
                        }
                    } else {
                        exchange.getMessage().setBody("{\"error\": \"Scheduler not available\"}");
                    }
                });

    }
}


// POJO for JSON serialization
class ScheduleStatus {
    private String scheduleId;
    private boolean active;
    private String nextRunTime;
    private String lastRunTime;

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getNextRunTime() {
        return nextRunTime;
    }

    public void setNextRunTime(String nextRunTime) {
        this.nextRunTime = nextRunTime;
    }

    public String getLastRunTime() {
        return lastRunTime;
    }

    public void setLastRunTime(String lastRunTime) {
        this.lastRunTime = lastRunTime;
    }
}