package edu.unc.mapseq.messaging;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.ws.SampleService;

public class NECSymlinkMessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.symlink");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s-%d\"}]}";
            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc", 67401,
                    "jdr-test-nec-variant-calling", 67401)));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testStressQueue() {
        QName serviceQName = new QName("http://ws.mapseq.unc.edu", "SampleService");
        QName portQName = new QName("http://ws.mapseq.unc.edu", "SamplePort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/SampleService", host, 8181));
        SampleService sampleService = service.getPort(SampleService.class);

        List<Sample> sampleList = new ArrayList<Sample>();

        sampleList.addAll(sampleService.findByFlowcellId(191541L));
        sampleList.addAll(sampleService.findByFlowcellId(191738L));
        // sampleList.addAll(sampleService.findByFlowcellId(190345L));
        // sampleList.addAll(sampleService.findByFlowcellId(190520L));
        // sampleList.addAll(sampleService.findByFlowcellId(191372L));
        // sampleList.addAll(sampleService.findByFlowcellId(192405L));
        // sampleList.addAll(sampleService.findByFlowcellId(191192L));

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.symlink");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s_L%d_%s_GATK\"}]}";
            for (Sample sample : sampleList) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                Flowcell flowcell = sample.getFlowcell();
                String message = String.format(format, sample.getId(), flowcell.getName(), sample.getLaneIndex(),
                        sample.getName());
                System.out.println(message);
                producer.send(session.createTextMessage(message));
            }

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testJSON() {

        try {
            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "Sample");
            generator.writeStringField("id", "<some_sample_id>");
            generator.writeArrayFieldStart("attributes");

            generator.writeStartObject();
            generator.writeStringField("name", "subjectName");
            generator.writeStringField("value", "<some_subject_name>");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("name", "qcPass");
            generator.writeStringField("value", "<true|false>");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "<some_workflow_run_name>");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();

            System.out.println(sw.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
