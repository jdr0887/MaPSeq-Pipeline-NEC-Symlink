package edu.unc.maspeq.commands.nec.symlink;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
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

    @Test
    public void migrateCASAVA() throws Exception {
        File tsv = new File("/home/jdr0887/sql/mapseq", "asdf.tsv");
        File sql = new File("/home/jdr0887/sql/mapseq", "asdf.sql");
        File sh = new File("/home/jdr0887/sql/mapseq", "asdf.sh");
        BufferedWriter sqlBW = new BufferedWriter(new FileWriter(sql));
        BufferedWriter shBW = new BufferedWriter(new FileWriter(sh));

        BufferedReader br = new BufferedReader(new FileReader(tsv));
        String line;
        while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line, " ");
            String id = st.nextToken();
            String path = st.nextToken();
            String file = st.nextToken();

            String tmp = file.substring(0, file.indexOf("."));

            if (tmp.indexOf("XX") == -1 || !file.endsWith("fastq.gz")) {
                continue;
            }

            StringBuilder sb = new StringBuilder(tmp);
            String flowcell = sb.substring(0, file.indexOf("XX") + 2);

            String barcode = sb.substring(flowcell.length() + 1, tmp.length());
            barcode = barcode.substring(0, barcode.indexOf("_"));

            String lane = sb.substring(sb.indexOf(barcode) + barcode.length() + 1, tmp.length());
            lane = lane.substring(0, lane.indexOf("_"));

            String newPath = String.format("/proj/seq/mapseq/RENCI/%s/%s_%s/CASAVA", flowcell, lane, barcode);
            sqlBW.write(String.format("update file_data set path = '%s' where id = %s;", newPath, id));
            sqlBW.newLine();
            sqlBW.flush();

            shBW.write(String.format("if [ -e %s/%s ]; then ", path, file));
            shBW.write(String.format("mkdir -p %s; ", newPath));
            shBW.write(String.format("mv %s/%s %s/; ", path, file, newPath));
            shBW.write("fi");
            shBW.newLine();
            shBW.flush();

        }

        br.close();
        sqlBW.flush();
        sqlBW.close();
        shBW.flush();
        shBW.close();
    }

    @Test
    public void migrateNEC() throws Exception {
        File tsv = new File("/home/jdr0887/sql/mapseq", "asdf.tsv");
        File sql = new File("/home/jdr0887/sql/mapseq", "asdf.sql");
        File sh = new File("/home/jdr0887/sql/mapseq", "asdf.sh");
        BufferedWriter sqlBW = new BufferedWriter(new FileWriter(sql));
        BufferedWriter shBW = new BufferedWriter(new FileWriter(sh));

        BufferedReader br = new BufferedReader(new FileReader(tsv));
        String line;
        while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line, " ");
            String id = st.nextToken();
            String path = st.nextToken();
            String file = st.nextToken();

            String tmp = file.substring(0, file.indexOf("."));

            StringBuilder sb = new StringBuilder(tmp);
            String flowcell = sb.substring(0, file.indexOf("XX") + 2);

            String barcode = sb.substring(flowcell.length() + 1, tmp.length());
            barcode = barcode.substring(0, barcode.indexOf("_"));

            String lane = sb.substring(sb.indexOf(barcode) + barcode.length() + 1, tmp.length());
            // lane = lane.substring(0, lane.indexOf("_"));

            String newPath = String.format("/proj/seq/mapseq/RENCI/%s/%s_%s/NEC", flowcell, lane, barcode);
            sqlBW.write(String.format("update file_data set path = '%s' where id = %s;", newPath, id));
            sqlBW.newLine();
            sqlBW.flush();

            shBW.write(String.format("if [ -e %s/%s ]; then ", path, file));
            shBW.write(String.format("mkdir -p %s; ", newPath));
            shBW.write(String.format("mv %s/%s %s/; ", path, file, newPath));
            shBW.write("fi");
            shBW.newLine();
            shBW.flush();

        }

        br.close();
        sqlBW.flush();
        sqlBW.close();
        shBW.flush();
        shBW.close();
    }

    @Test
    public void migrateNCGenes() throws Exception {
        File tsv = new File("/home/jdr0887/sql/mapseq", "asdf.tsv");
        File sql = new File("/home/jdr0887/sql/mapseq", "asdf.sql");
        File sh = new File("/home/jdr0887/sql/mapseq", "asdf.sh");
        BufferedWriter sqlBW = new BufferedWriter(new FileWriter(sql));
        BufferedWriter shBW = new BufferedWriter(new FileWriter(sh));

        BufferedReader br = new BufferedReader(new FileReader(tsv));
        String line;
        while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line, " ");
            String id = st.nextToken();
            String path = st.nextToken();
            String file = st.nextToken();

            String tmp = file.substring(0, file.indexOf("."));

            StringBuilder sb = new StringBuilder(tmp);
            String flowcell = sb.substring(0, file.indexOf("XX") + 2);

            String barcode = sb.substring(flowcell.length() + 1, tmp.length());
            barcode = barcode.substring(0, barcode.indexOf("_"));

            String lane = sb.substring(sb.indexOf(barcode) + barcode.length() + 1, tmp.length());
            // lane = lane.substring(0, lane.indexOf("_"));

            String newPath = String.format("/proj/seq/mapseq/RENCI/%s/%s_%s/NCGenes", flowcell, lane, barcode);
            sqlBW.write(String.format("update file_data set path = '%s' where id = %s;", newPath, id));
            sqlBW.newLine();
            sqlBW.flush();

            shBW.write(String.format("if [ -e %s/%s ]; then ", path, file));
            shBW.write(String.format("mkdir -p %s; ", newPath));
            shBW.write(String.format("mv %s/%s %s/; ", path, file, newPath));
            shBW.write("fi");
            shBW.newLine();
            shBW.flush();

        }

        br.close();
        sqlBW.flush();
        sqlBW.close();
        shBW.flush();
        shBW.close();
    }

    @Test
    public void id2idMapping() throws Exception {
        File htsfSampleIdListFile = new File("/home/jdr0887/sql/tmp", "htsfSampleIdList.txt");
        List<String> mapseq4Lines = FileUtils.readLines(htsfSampleIdListFile);

        File sampleIdListFile = new File("/home/jdr0887/sql/tmp", "sampleIdList.txt");
        List<String> mapseq5Lines = FileUtils.readLines(sampleIdListFile);

        File mapping = new File("/home/jdr0887/sql/tmp", "mapping.tsv");
        BufferedWriter bw = new BufferedWriter(new FileWriter(mapping));

        for (String mps5line : mapseq5Lines) {

            StringTokenizer mps5ST = new StringTokenizer(mps5line, " | ");
            String mps5Id = mps5ST.nextToken().trim();
            String mps5Name = mps5ST.nextToken().trim();
            String mps5Lane = mps5ST.nextToken().trim();
            String mps5Barcode = mps5ST.nextToken().trim();

            for (String mps4line : mapseq4Lines) {
                StringTokenizer mps4ST = new StringTokenizer(mps4line, " | ");
                String mps4Id = mps4ST.nextToken().trim();
                String mps4Name = mps4ST.nextToken().trim();
                String mps4Lane = mps4ST.nextToken().trim();
                String mps4Barcode = mps4ST.nextToken().trim();

                if (mps5Name.equals(mps4Name) && mps5Lane.equals(mps4Lane) && mps5Barcode.equals(mps4Barcode)) {
                    bw.write(String.format("%s:%s", mps4Id, mps5Id));
                    bw.newLine();
                    bw.flush();
                }

            }

        }

        bw.flush();
        bw.close();
    }

}
