package edu.unc.mapseq.messaging;

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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.ws.HTSFSampleService;

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
            String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"HTSFSample\",\"guid\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s-%d\"}]}";
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
        QName serviceQName = new QName("http://ws.mapseq.unc.edu", "HTSFSampleService");
        QName portQName = new QName("http://ws.mapseq.unc.edu", "HTSFSamplePort");
        Service service = Service.create(serviceQName);
        String host = "biodev2.its.unc.edu";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/HTSFSampleService", host, 8181));
        HTSFSampleService htsfSampleService = service.getPort(HTSFSampleService.class);

        List<HTSFSample> sampleList = new ArrayList<HTSFSample>();

        sampleList.addAll(htsfSampleService.findBySequencerRunId(191541L));
        sampleList.addAll(htsfSampleService.findBySequencerRunId(191738L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(190345L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(190520L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(191372L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(192405L));
        // sampleList.addAll(htsfSampleService.findBySequencerRunId(191192L));

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
            String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"HTSFSample\",\"guid\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s_L%d_%s_GATK\"}]}";
            for (HTSFSample sample : sampleList) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                SequencerRun sr = sample.getSequencerRun();
                String message = String.format(format, "rc_renci.svc", sample.getId(), sr.getName(),
                        sample.getLaneIndex(), sample.getName());
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
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("account_name", "rc_renci.svc");

            JSONArray entityArray = new JSONArray();

            JSONObject htsfEntityObject = new JSONObject();
            htsfEntityObject.put("entity_type", "HTSFSample");
            htsfEntityObject.put("guid", "<some_sample_id>");

            JSONArray entityAttributeArray = new JSONArray();

            JSONObject htsfEntityAttributeObject = new JSONObject();
            htsfEntityAttributeObject.put("name", "subjectName");
            htsfEntityAttributeObject.put("value", "<some_subject_name>");
            entityAttributeArray.put(htsfEntityAttributeObject);

            htsfEntityAttributeObject = new JSONObject();
            htsfEntityAttributeObject.put("name", "qcPass");
            htsfEntityAttributeObject.put("value", "<true|false>");
            entityAttributeArray.put(htsfEntityAttributeObject);

            htsfEntityObject.put("attributes", entityAttributeArray);

            entityArray.put(htsfEntityObject);

            JSONObject workflowRunEntityObject = new JSONObject();
            workflowRunEntityObject.put("entity_type", "WorkflowRun");
            workflowRunEntityObject.put("name", "<some_workflow_run_name>");
            entityArray.put(workflowRunEntityObject);

            jsonObject.put("entities", entityArray);

            System.out.println(jsonObject.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

}