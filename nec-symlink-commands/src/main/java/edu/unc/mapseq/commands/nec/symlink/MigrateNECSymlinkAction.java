package edu.unc.mapseq.commands.nec.symlink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.commands.Option;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.StudyDAO;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.workflow.WorkflowUtil;

@Command(scope = "nec-symlink", name = "migrate", description = "Migrate NEC Symlink")
public class MigrateNECSymlinkAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(MigrateNECSymlinkAction.class);

    @Option(name = "--dryRun", description = "Don't move anything", required = false, multiValued = false)
    private Boolean dryRun = Boolean.FALSE;

    private MaPSeqDAOBean maPSeqDAOBean;

    public MigrateNECSymlinkAction() {
        super();
    }

    @Override
    public Object doExecute() {

        FlowcellDAO flowcellDAO = maPSeqDAOBean.getFlowcellDAO();
        StudyDAO studyDAO = maPSeqDAOBean.getStudyDAO();
        SampleDAO sampleDAO = maPSeqDAOBean.getSampleDAO();

        try {
            List<Flowcell> flowcells = flowcellDAO.findAll();

            List<String> lines = IOUtils.readLines(getClass().getClassLoader().getResourceAsStream(
                    "edu/unc/mapseq/commands/nec/symlink/SubjectToBarcodeMap.csv"));

            Study study = null;
            try {
                List<Study> studyList = studyDAO.findByName("NEC");
                if (studyList != null && !studyList.isEmpty()) {
                    study = studyList.get(0);
                }
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            if (study == null) {
                logger.error("Couldn't find NEC Study");
                return null;
            }

            File cohortDirectory = new File("/proj/renci/sequence_analysis", "NEC");

            if (flowcells != null && !flowcells.isEmpty()) {

                for (Flowcell flowcell : flowcells) {

                    logger.debug(flowcell.toString());

                    List<Sample> samples = sampleDAO.findByFlowcellId(flowcell.getId());

                    if (samples != null && !samples.isEmpty()) {

                        for (Sample sample : samples) {

                            if (!study.equals(sample.getStudy()) || "Undetermined".equals(sample.getBarcode())) {
                                continue;
                            }

                            logger.debug(sample.toString());

                            String subjectName = null;

                            // find subjectName
                            for (String line : lines) {
                                if (line.contains(flowcell.getName()) && line.contains(sample.getName())) {
                                    subjectName = line.split(",")[0];
                                }
                            }

                            if (StringUtils.isEmpty(subjectName)) {
                                continue;
                            }

                            List<File> readPairList = WorkflowUtil.getReadPairList(sample.getFileDatas(),
                                    flowcell.getName(), sample.getLaneIndex());

                            // error check
                            if (readPairList.size() != 2) {
                                logger.warn("readPairList.size(): {}", readPairList.size());
                                return null;
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
                                // Files.deleteIfExists(qcFailProjDirectory.toPath());
                                // Files.createSymbolicLink(qcFailProjDirectory.toPath(), outputDirectory.toPath());
                                // return;
                                // }

                                File fastqR1Symlink = new File(sequenceProjDirectory, r1FastqFile.getName());
                                logger.info(String.format("linking %s to %s", r1FastqFile.getAbsolutePath(),
                                        fastqR1Symlink.getAbsolutePath()));
                                if (!dryRun) {
                                    Files.deleteIfExists(fastqR1Symlink.toPath());
                                    Files.createSymbolicLink(fastqR1Symlink.toPath(), r1FastqFile.toPath());
                                }

                                File fastqR2Symlink = new File(sequenceProjDirectory, r2FastqFile.getName());
                                logger.info(String.format("linking %s to %s", r2FastqFile.getAbsolutePath(),
                                        fastqR2Symlink.getAbsolutePath()));
                                if (!dryRun) {
                                    Files.deleteIfExists(fastqR2Symlink.toPath());
                                    Files.createSymbolicLink(fastqR2Symlink.toPath(), r2FastqFile.toPath());
                                }
                                // cycle through all files in the analysisWorkflowDirectory
                                File necOutputDir = new File(sample.getOutputDirectory(), "NEC");

                                for (File f : necOutputDir.listFiles()) {
                                    String fname = f.getName();

                                    if (fname.endsWith("fastqc.zip")) {

                                        File targetFile = new File(sequenceProjDirectory, fname);
                                        logger.info(String.format("linking %s to %s", targetFile.getAbsolutePath(),
                                                f.getAbsolutePath()));
                                        if (!dryRun) {
                                            Files.deleteIfExists(f.toPath());
                                            Files.createSymbolicLink(f.toPath(), targetFile.toPath());
                                        }
                                    } else if (fname.endsWith("fixed-rg.bam") || fname.endsWith("fixed-rg.bai")) {

                                        File targetFile = new File(alignmentProjDirectory, fname);
                                        logger.info(String.format("linking %s to %s", targetFile.getAbsolutePath(),
                                                f.getAbsolutePath()));
                                        if (!dryRun) {
                                            Files.deleteIfExists(f.toPath());
                                            Files.createSymbolicLink(f.toPath(), targetFile.toPath());
                                        }
                                    } else if (fname.contains(".coverage.") || fname.endsWith("flagstat")) {

                                        File targetFile = new File(alignmentStatProjDirectory, fname);
                                        logger.info(String.format("linking %s to %s", targetFile.getAbsolutePath(),
                                                f.getAbsolutePath()));
                                        if (!dryRun) {
                                            Files.deleteIfExists(f.toPath());
                                            Files.createSymbolicLink(f.toPath(), targetFile.toPath());
                                        }
                                    } else if (fname.endsWith("fvcf")) {

                                        File targetFile = new File(idchkFVCFProjDirectory, fname);
                                        logger.info(String.format("linking %s to %s", targetFile.getAbsolutePath(),
                                                f.getAbsolutePath()));
                                        if (!dryRun) {
                                            Files.deleteIfExists(f.toPath());
                                            Files.createSymbolicLink(f.toPath(), targetFile.toPath());
                                        }
                                    } else if (fname.endsWith("ec.tsv")) {

                                        File targetFile = new File(idchkECProjDirectory, fname);
                                        logger.info(String.format("linking %s to %s", targetFile.getAbsolutePath(),
                                                f.getAbsolutePath()));
                                        if (!dryRun) {
                                            Files.deleteIfExists(f.toPath());
                                            Files.createSymbolicLink(f.toPath(), targetFile.toPath());
                                        }
                                    }

                                }

                            } catch (Exception e) {
                                logger.error("Exception", e);
                            }

                        }

                    }

                }

            }
        } catch (MaPSeqDAOException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(Boolean dryRun) {
        this.dryRun = dryRun;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

}
