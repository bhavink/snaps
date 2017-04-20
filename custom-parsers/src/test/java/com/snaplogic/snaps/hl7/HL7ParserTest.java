package com.snaplogic.snaps.hl7;
import com.snaplogic.snap.test.harness.OutputRecorder;
import com.snaplogic.snap.test.harness.SnapTestRunner;
import com.snaplogic.snap.test.harness.TestFixture;
import com.snaplogic.snap.test.harness.TestResult;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the {@link com.snaplogic.snaps.hl7.HL7Parser} Snap sent one Document to the output view.
 */
@RunWith(SnapTestRunner.class)
public class HL7ParserTest {

    @TestFixture(snap = HL7Parser.class,
            input = "data/hl7_sample_input.data"
            ,outputs = "output0")
    public void testSingleDocGeneratorFunctionality(TestResult testResult)
            throws Exception {
        assertNull(testResult.getException());
        OutputRecorder outputRecorder = testResult.getOutputViewByName("output0");
        assertEquals(1, outputRecorder.getDocumentCount());
    }
}
