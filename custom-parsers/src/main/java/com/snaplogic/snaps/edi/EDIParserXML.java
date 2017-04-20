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
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.BinaryOutput;
import com.snaplogic.snap.api.PropertyValues;
import com.snaplogic.snap.api.SnapCategory;
import com.snaplogic.snap.api.SnapDataException;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snap.api.write.SimpleBinaryWriteSnap;
import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.input.SAXHandler;
import org.jdom.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static com.berryworks.edireader.EDIReaderFactory.createEDIReader;

/**
 * Created by bkukadia on 7/20/2016.
 */

@General(title = "EDI to XML", purpose = "Parse EDI Messages (Field Snap) to XML", author = "SnapLogic")
@Inputs(min = 1, max = 1, accepts = {ViewType.BINARY})
@Outputs(min = 1, max = 1, offers = {ViewType.BINARY})
@Errors(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.PARSE)
public class EDIParserXML extends SimpleBinaryWriteSnap {
    private static final Logger log = LoggerFactory.getLogger(EDIParserXML.class);

    private static final String UTF_8 = "UTF-8";
    @Override
    protected void process(final com.snaplogic.snap.api.Document header, final ReadableByteChannel readChannel) {
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


        outputViews.write(new BinaryOutput() {
            @Override
            public com.snaplogic.snap.api.Document getHeader() {
                // maintain the incoming header
                return header;
            }
            @Override
            public void write(WritableByteChannel writeChannel) throws IOException {
                OutputStream outputStream = Channels.newOutputStream(writeChannel);
                try {
                    IOUtils.write(transformedData.toString(), outputStream, UTF_8);
                } finally {
                    IOUtils.closeQuietly(outputStream);
                }
            }

        });
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
}