/*
 * SnapLogic - Data Integration
 *
 * Copyright (C) 2017, SnapLogic, Inc.  All rights reserved.
 *
 * This program is licensed under the terms of
 * the SnapLogic Commercial Subscription agreement.
 *
 * "SnapLogic" is a trademark of SnapLogic, Inc.
 */

package com.snaplogic.snaps.marc21;


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
import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.input.SAXHandler;
import org.jdom.output.XMLOutputter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.converter.impl.AnselToUnicode;
import org.marc4j.marc.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import javax.xml.transform.Result;
import javax.xml.transform.sax.SAXResult;



/**
 * Created by bkukadia on 03/23/2017.
 */

@General(title = "MARC21 Parser", purpose = "Parse Marc21 Messages (Field Snap) to XML", author = "SnapLogic")
@Inputs(min = 1, max = 1, accepts = {ViewType.BINARY})
@Outputs(min = 1, max = 1, offers = {ViewType.BINARY})
@Errors(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.PARSE)
public class Marc21toXMLParser extends SimpleBinaryWriteSnap {
    private static final Logger log = LoggerFactory.getLogger(Marc21toXMLParser.class);

    private static final String UTF_8 = "UTF-8";
    @Override
    protected void process(final com.snaplogic.snap.api.Document header, final ReadableByteChannel readChannel) {
        //String transformedData = null;
        final StringBuilder transformedData = new StringBuilder();
        OutputStream outputStream = null;

        try (InputStream inputStream = Channels.newInputStream(readChannel)) {
            //outputStream = Channels.newOutputStream(writeChannel);
            // If you want to read as char stream rather than byte stream
            // Reader reader = new InputStreamReader(new BufferedInputStream(inputStream), UTF_8);
           
            MarcReader reader = new MarcStreamReader(inputStream);
            OutputFormat format = new OutputFormat("xml","UTF-8", true);
           
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream out = baos;
            XMLSerializer serializer = new XMLSerializer(out, format);
            Result result = new SAXResult(serializer.asContentHandler());
            
            MarcXmlWriter writer = new MarcXmlWriter(result);
            writer.setConverter(new AnselToUnicode());
            while (reader.hasNext()) {
                Record record = reader.next();
                writer.write(record);
            }
            writer.close();
            
            transformedData.append(baos.toString());
            //IOUtils.write(transformedData, outputStream, UTF_8);
        }catch (Exception e) {
            log.debug("" + e.getMessage() + " " + e.getStackTrace());
            //Stop's snap execution
            //throw new ExecutionException(e, "Failed to parse MARC21 message data").withReason(Throwables.getRootCause(e).getMessage()).withResolutionAsDefect();
            SnapDataException snapDataException = new SnapDataException(
                    e,
                    "Error/Exception parsing MARC21 file"
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