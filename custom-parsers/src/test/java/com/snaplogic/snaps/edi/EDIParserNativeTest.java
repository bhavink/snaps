/**
 * Created by bkukadia on 10/11/2016.
 */
package com.snaplogic.snaps.edi;

import com.snaplogic.snap.test.harness.OutputRecorder;
import com.snaplogic.snap.test.harness.SnapTestRunner;
import com.snaplogic.snap.test.harness.TestFixture;
import com.snaplogic.snap.test.harness.TestResult;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the {@link EDIParser} Snap sent one Document to the output view.
 */
@RunWith(SnapTestRunner.class)
public class EDIParserNativeTest {

    @TestFixture(snap = EDIParser.class,
            input = "data/edi_sample_input.data"
            ,outputs = "output0")
    public void testSingleDocGeneratorFunctionality(TestResult testResult)
            throws Exception {
        assertNull(testResult.getException());
        OutputRecorder outputRecorder = testResult.getOutputViewByName("output0");
        assertEquals(1, outputRecorder.getDocumentCount());
    }
}
