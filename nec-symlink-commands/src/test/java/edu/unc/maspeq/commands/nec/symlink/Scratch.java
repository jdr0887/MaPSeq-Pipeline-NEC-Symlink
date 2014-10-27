package edu.unc.maspeq.commands.nec.symlink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
    

    @Test
    public void testSymlink() {
        File link = new File("/home/jdr0887/tmp/canvas", "condor.dag.linked");
        File target = new File("/home/jdr0887/tmp", "condor.dag");
        
        try {
            System.out.println(link.toPath().getParent().toRealPath());
            Files.deleteIfExists(link.toPath());
            Files.createSymbolicLink(link.toPath(), target.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    
}
