package com.snaplogic.snaps.binary.checksum;

/**
 * Created by bkukadia on 11/3/2016.
 */

import com.snaplogic.api.ConfigurationException;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.*;
import com.snaplogic.snap.api.capabilities.*;
import com.snaplogic.snap.view.InputView;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


@General(title = "Checksum", purpose = "Calculate Binary File MD5 Checksum (Field Snap)", author = "SnapLogic")
@Inputs(min = 1, max = 1, accepts = {ViewType.BINARY})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Errors(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.PARSE)


public class BinaryChecksum extends SimpleBinarySnap {

    private static final Logger log = LoggerFactory.getLogger(BinaryChecksum.class);

    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {
    }

    @Override
    public void cleanup() throws ExecutionException {
    }

    @Override
    protected void doWork() {
        // do nothing if input view is not connected
        if (inputViews.getAll().isEmpty()) {
            return;
        }

        try {
            final InputView inputView = inputViews.get();
            final Iterator<BinaryInput> binaryDataIterator = inputViews.getBinaryInputsFrom(inputView);
            while (binaryDataIterator.hasNext()) {

                InputStream inputStream = null;
                try {
                    final BinaryInput binaryInput = binaryDataIterator.next();
                    if (binaryInput == null) {
                        continue;
                    }
                    inputStream = getInputStream(binaryInput);
                    String md5 = DigestUtils.md5Hex(inputStream);
                 //   String md2 = DigestUtils.md2Hex(inputStream);
                 //   String sha512 = DigestUtils.sha512Hex(inputStream);
                 //   String sha384 = DigestUtils.sha384Hex(inputStream);
                 //   String sha1 = DigestUtils.sha1Hex(inputStream);
                 //   String sha256 = DigestUtils.sha256Hex(inputStream);


                    Map<String, String> data = new LinkedHashMap<>();
                    data.put("MD5", md5);
                 //   data.put("MD2",md2);
                 //   data.put("SHA1",sha1);
                 //   data.put("SHA512",sha512);
                 //   data.put("SHA256",sha256);
                 //   data.put("SHA384",sha384);


                    outputViews.write(documentUtility.newDocument(data));
                } finally {
                    IOUtils.closeQuietly(inputStream);
                }
            }
        } catch (Exception e) {
            SnapDataException snapDataException = new SnapDataException(
                    e,
                    "Error reading data"
            );
            errorViews.write(snapDataException);
        }

    }




    private InputStream getInputStream(final BinaryInput binaryInput) throws IOException {
        return Channels.newInputStream(binaryInput.getChannel());
    }


}
