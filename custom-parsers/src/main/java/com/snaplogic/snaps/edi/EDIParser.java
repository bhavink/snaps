/*
 * SnapLogic - Data Integration
 *
 * Copyright (C) 2013, SnapLogic, Inc.  All rights reserved.
 *
 * This program is licensed under the terms of
 * the SnapLogic Commercial Subscription agreement.
 *
 * "SnapLogic" is a trademark of SnapLogic, Inc.
 */

package com.snaplogic.snaps.edi;

import com.berryworks.edireader.EDIReader;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.DependencyManager;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.PropertyValues;
import com.snaplogic.snap.api.SnapCategory;
import com.snaplogic.snap.api.SnapDataException;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snap.api.write.SimpleBinaryWriteSnap;
import com.snaplogic.snap.api.xml.XmlUtils;
import com.snaplogic.snap.api.xml.XmlUtilsImpl;
import de.odysseus.staxon.json.JsonXMLConfig;
import de.odysseus.staxon.json.JsonXMLConfigBuilder;
import de.odysseus.staxon.json.JsonXMLOutputFactory;
import org.jdom.Document;
import org.jdom.input.SAXHandler;
import org.jdom.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import static com.berryworks.edireader.EDIReaderFactory.createEDIReader;

/**
 * Created by bkukadia on 7/20/2016.
 */

@General(title = "EDI Parser", purpose = "Parse EDI Messages to JSON documents (Field Snap)", author = "SnapLogic")
@Inputs(min = 1, max = 1, accepts = {ViewType.BINARY})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Errors(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.PARSE)
public class EDIParser extends SimpleBinaryWriteSnap implements DependencyManager {
    private static final Logger log = LoggerFactory.getLogger(EDIParser.class);

    private static final String UTF_8 = "UTF-8";


    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    private final JsonXMLConfig config = new JsonXMLConfigBuilder()
            .autoArray(true)
            .multiplePI(true)
            .build();
    private final JsonXMLOutputFactory jsonOutputFactory = new JsonXMLOutputFactory(config);
    StringBuilder transformedData = null;
    String ackMessageInXML = "";

    @Inject
    private XmlUtils XMLUtils;


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
    protected void process(final com.snaplogic.snap.api.Document header, final ReadableByteChannel readChannel) {
        Source source = null;
        //String transformedData = null;
        InputSource inputSource;
        final StringBuilder transformedData = new StringBuilder();
        //OutputStream outputStream = null;

        try (InputStream inputStream = Channels.newInputStream(readChannel)) {
            //outputStream = Channels.newOutputStream(writeChannel);
            // If you want to read as char stream rather than byte stream
            // Reader reader = new InputStreamReader(new BufferedInputStream(inputStream), UTF_8);
            inputSource = new InputSource(inputStream);
            EDIReader parser = createEDIReader(inputSource);
            SAXHandler handler = new SAXHandler();
            parser.setContentHandler(handler);
            parser.parse(inputSource);
            Document doc = handler.getDocument();
            // Output as XML
            XMLOutputter outputter = new XMLOutputter();
            transformedData.append(outputter.outputString(doc));

            source = new StreamSource(new StringReader(transformedData.toString()));
            Object resultFromXML = XMLUtils.convertToJson(xmlInputFactory, jsonOutputFactory, source);

            if (resultFromXML != null) {
                writeToOutputViews(header, resultFromXML);
                transformedData.setLength(0);
            }

            transformedData.setLength(0);

            //IOUtils.write(transformedData, outputStream, UTF_8);
        }catch (Exception e) {
            log.debug("" + e.getMessage() + " " + e.getStackTrace());
            //Stop's snap execution
            //throw new ExecutionException(e, "Failed to parse hl7 message data").withReason(Throwables.getRootCause(e).getMessage()).withResolutionAsDefect();
            SnapDataException snapDataException = new SnapDataException(
                    e,
                    "Error/Exception parsing EDI file"
            ).withReason(Throwables.getRootCause(e).getMessage()).withResolutionAsDefect();

            errorViews.write(snapDataException);
            return;

        }



    }

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {

    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {

    }

    @Override
    public void cleanup() throws ExecutionException {

    }
    private void writeToOutputViews(final com.snaplogic.snap.api.Document header, final Object data) {
        outputViews.write(documentUtility.newDocumentFor(header, data));
    }
}