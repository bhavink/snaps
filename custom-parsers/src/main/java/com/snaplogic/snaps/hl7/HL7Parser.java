package com.snaplogic.snaps.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.ACK;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.parser.GenericModelClassFactory;
import ca.uhn.hl7v2.parser.ModelClassFactory;
import ca.uhn.hl7v2.parser.XMLParser;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import ca.uhn.hl7v2.validation.builder.support.NoValidationBuilder;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.DependencyManager;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snap.api.xml.XmlUtils;
import com.snaplogic.snap.api.xml.XmlUtilsImpl;
import com.snaplogic.snap.view.InputView;
import de.odysseus.staxon.json.JsonXMLConfig;
import de.odysseus.staxon.json.JsonXMLConfigBuilder;
import de.odysseus.staxon.json.JsonXMLOutputFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.util.Iterator;


/**
 * Created by bkukadia on 10/7/2016.
 */

@General(title = "HL7 Parser", purpose = "Parse HL7 messages (Field Snap)", author = "SnapLogic")
@Inputs(min = 1, max = 1, accepts = {ViewType.BINARY})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Errors(max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.PARSE)

public class HL7Parser extends SimpleBinarySnap implements DependencyManager{

    @Inject
    private XmlUtils XMLUtils;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(HL7Parser.class);


    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    private final JsonXMLConfig config = new JsonXMLConfigBuilder()
            .autoArray(true)
            .multiplePI(true)
            .build();
    private final JsonXMLOutputFactory jsonOutputFactory = new JsonXMLOutputFactory(config);
    StringBuilder transformedData = null;
    String ackMessageInXML = "";

    @Inject
    private InputViews inputViews;




    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {

    }


    @Override
    public Module getManagedModule() {
        return new AbstractModule() {
            @Override
            protected void configure() {
                bind(XmlUtils.class).to(XmlUtilsImpl.class);
            }
        };
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {

    }

    @Override
    public void cleanup() throws ExecutionException {

    }




    @Override
    protected void doWork() {
        Source source = null;
        HapiContext context = new DefaultHapiContext();
        context.setValidationRuleBuilder(new NoValidationBuilder());
        context.getParserConfiguration().setAllowUnknownVersions(true);
        XMLParser xmlParser = new DefaultXMLParser(context);
        ModelClassFactory modelClassFactory = new GenericModelClassFactory();
        InputStream inputStream = null;

        // do nothing if input view is not connected
        if (inputViews.getAll().isEmpty()) {
            return;
        }

        try {
            final InputView inputView = inputViews.get();
            final Iterator<BinaryInput> binaryDataIterator = inputViews.getBinaryInputsFrom(inputView);
            while (binaryDataIterator.hasNext()) {
                try {
                    final BinaryInput binaryInput = binaryDataIterator.next();

                    if (binaryInput == null) {

                        continue;
                    }

                    inputStream = getInputStream(binaryInput);
                    System.out.println("binary is null2");
                    Hl7InputStreamMessageIterator iter = new Hl7InputStreamMessageIterator(inputStream);

                    while (iter.hasNext()) {
                        transformedData = new StringBuilder();
                        Message message = iter.next();
                        if (message instanceof ACK) {
                            ACK ack = (ACK) message;
                            ack.getMSH().getProcessingID().getProcessingMode().setValue("P");
                        }
                        // Do something with the message
                        ackMessageInXML = xmlParser.encode(message);
                        source = new StreamSource(new StringReader(ackMessageInXML.replaceAll("xmlns=\"urn:hl7-org:v2xml\"", "xmlns:t=\"urn:hl7-org:v2xml\"")));
                        Object resultFromXML = XMLUtils.convertToJson(xmlInputFactory, jsonOutputFactory, source);
                        Document header = binaryInput.getHeader();
                        if (resultFromXML != null) {
                            writeToOutputViews(header, resultFromXML);
                            transformedData.setLength(0);
                        }
                        transformedData.setLength(0);
                    }
                }

                catch (Exception e) {
                    LOGGER.debug("" + e.getMessage() + " " + e.getStackTrace());
                    //Stop's snap execution
                    //throw new ExecutionException(e, "Failed to parse hl7 message data").withReason(Throwables.getRootCause(e).getMessage()).withResolutionAsDefect();
                    SnapDataException snapDataException = new SnapDataException(
                            e,
                            "Error/Exception parsing HL7 data - " + ackMessageInXML
                    ).withReason(Throwables.getRootCause(e).getMessage()).withResolutionAsDefect();

                    errorViews.write(snapDataException);
                    return;


                }

            }


        } finally {

            if (inputStream != null) {
                // call close on the wrapped one only
                IOUtils.closeQuietly(inputStream);
            }


        }

    }

    private void writeToOutputViews(final Document header, final Object data) {
        outputViews.write(documentUtility.newDocumentFor(header, data));
    }

    private InputStream getInputStream(final BinaryInput binaryInput) throws IOException {
        return Channels.newInputStream(binaryInput.getChannel());
    }


}