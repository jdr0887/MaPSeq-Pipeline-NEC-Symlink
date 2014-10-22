package edu.unc.maspeq.commands.nec.symlink;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class Scratch {

    @Test
    public void scratch() {

        try {
            List<String> lines = IOUtils.readLines(getClass().getClassLoader().getResourceAsStream(
                    "edu/unc/mapseq/commands/nec/symlink/SubjectToBarcodeMap.csv"));
            assertTrue(lines != null && !lines.isEmpty());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
