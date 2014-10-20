package edu.unc.mapseq.commons.nec.symlink;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class SymlinkRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(SymlinkRunnable.class);

    private MaPSeqDAOBean mapseqDAOBean;

    private Sample sample;

    private String cohortDirectory;

    private Boolean qcPass = Boolean.FALSE;

    @Override
    public void run() {
        SampleDAO sampleDAO = mapseqDAOBean.getSampleDAO();
        AttributeDAO attributeDAO = mapseqDAOBean.getAttributeDAO();
        WorkflowDAO workflowDAO = mapseqDAOBean.getWorkflowDAO();

        String subjectName = null;

        Flowcell flowcell = sample.getFlowcell();

        List<File> readPairList = WorkflowUtil.getReadPairList(sample.getFileDatas(), flowcell.getName(),
                sample.getLaneIndex());

        // error check
        if (readPairList.size() != 2) {
            logger.warn("readPairList.size(): {}", readPairList.size());
            return;
        }

        // fastq file names
        File r1FastqFile = readPairList.get(0);
        File r2FastqFile = readPairList.get(1);

        try {

            // project directories
            File subjectParentDirectory = new File(cohortDirectory, "subjects");
            File subjectDirectory = new File(subjectParentDirectory, subjectName);

            File sequenceProjDirectory = new File(subjectDirectory, "sequence");
            if (!sequenceProjDirectory.exists()) {
                sequenceProjDirectory.mkdirs();
            }

            File alignmentProjDirectory = new File(subjectDirectory, "alignment");
            File alignmentStatProjDirectory = new File(alignmentProjDirectory, "stats");
            if (!alignmentStatProjDirectory.exists()) {
                alignmentStatProjDirectory.mkdirs();
            }

            File qcFailProjDirectory = new File(subjectDirectory, "qc_fail");
            if (!qcFailProjDirectory.exists()) {
                qcFailProjDirectory.mkdirs();
            }

            File idchkProjDirectory = new File(cohortDirectory, "idchk");

            File idchkFVCFProjDirectory = new File(idchkProjDirectory, "fvcf");
            if (!idchkFVCFProjDirectory.exists()) {
                idchkFVCFProjDirectory.mkdirs();
            }

            File idchkECProjDirectory = new File(idchkProjDirectory, "ec");
            if (!idchkECProjDirectory.exists()) {
                idchkECProjDirectory.mkdirs();
            }

            // // don't link all files, just the analysis directory
            // if (!qcPass) {
            //
            // Files.createSymbolicLink(qcFailProjDirectory.toPath(), outputDirectory.toPath());
            //
            // CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, SymlinkCLI.class, attempt.getId())
            // .siteName(siteName);
            // builder.addArgument(SymlinkCLI.TARGET, outputDirectory.getAbsolutePath())
            // .addArgument(SymlinkCLI.LINK, qcFailProjDirectory.getAbsolutePath())
            // .addArgument(SymlinkCLI.SLEEPDURATION, "3");
            // CondorJob job = builder.build();
            // logger.info(job.toString());
            // graph.addVertex(job);
            // continue;
            // }
            //
            // String format = "%s,%s";
            // List<String> targetLinkPairList = new ArrayList<String>();
            // File fastqR1Symlink = new File(sequenceProjDirectory, r1FastqFile.getName());
            // targetLinkPairList.add(String.format(format, r1FastqFile.getAbsolutePath(),
            // fastqR1Symlink.getAbsolutePath()));
            // File fastqR2Symlink = new File(sequenceProjDirectory, r2FastqFile.getName());
            // targetLinkPairList.add(String.format(format, r2FastqFile.getAbsolutePath(),
            // fastqR2Symlink.getAbsolutePath()));
            //
            // // cycle through all files in the analysisWorkflowDirectory
            // for (File f : outputDirectory.listFiles()) {
            // String fname = f.getName();
            //
            // if (fname.endsWith("fastqc.zip")) {
            //
            // File targetFile = new File(sequenceProjDirectory, fname);
            // targetLinkPairList.add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
            //
            // } else if (fname.endsWith("fixed-rg.bam") || fname.endsWith("fixed-rg.bai")) {
            //
            // File targetFile = new File(alignmentProjDirectory, fname);
            // targetLinkPairList.add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
            //
            // } else if (fname.contains(".coverage.") || fname.endsWith("flagstat")) {
            //
            // File targetFile = new File(alignmentStatProjDirectory, fname);
            // targetLinkPairList.add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
            //
            // } else if (fname.endsWith("fvcf")) {
            //
            // File targetFile = new File(idchkFVCFProjDirectory, fname);
            // targetLinkPairList.add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
            //
            // } else if (fname.endsWith("ec.tsv")) {
            //
            // File targetFile = new File(idchkECProjDirectory, fname);
            // targetLinkPairList.add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
            //
            // }
            //
            // }

        } catch (Exception e) {
        }

    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public Sample getSample() {
        return sample;
    }

    public void setSample(Sample sample) {
        this.sample = sample;
    }

}
