package edu.unc.mapseq.workflow.nec.symlink;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.BatchSymlinkCLI;
import edu.unc.mapseq.module.core.SymlinkCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NECSymlinkWorkflow extends AbstractSampleWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NECSymlinkWorkflow.class);

    public NECSymlinkWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NECSymlinkWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/nec/symlink/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String saHome = getWorkflowBeanService().getAttributes().get("sequenceAnalysisHome");

        File cohortDirectory = new File(saHome, "NEC");

        logger.info("cohortDirectory.getAbsolutePath(): {}", cohortDirectory.getAbsolutePath());

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.info(sample.toString());

            String subjectName = null;
            boolean qcPass = false;

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
                    String name = attribute.getName();
                    String value = attribute.getValue();
                    if ("subjectName".equals(name)) {
                        subjectName = value;
                    }
                    if ("qcPass".equals(name) && StringUtils.isNotEmpty(value) && "true".equalsIgnoreCase(value)) {
                        qcPass = true;
                    }
                }
            }
            if (StringUtils.isEmpty(subjectName)) {
                throw new WorkflowException("invalid subjectName");
            }

            File necAlignmentDirectory = new File(sample.getOutputDirectory(), "NECAlignment");
            File necVariantCallingDirectory = new File(sample.getOutputDirectory(), "NECVariantCalling");
            File necIDCheckDirectory = new File(sample.getOutputDirectory(), "NECIDCheck");
            File tmpDirectory = new File(sample.getOutputDirectory(), "tmp");
            tmpDirectory.mkdirs();

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

                // don't link all files, just the analysis directory
                if (!qcPass) {

                    CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, SymlinkCLI.class, attempt.getId())
                            .siteName(siteName);
                    builder.addArgument(SymlinkCLI.TARGET, sample.getOutputDirectory())
                            .addArgument(SymlinkCLI.LINK, qcFailProjDirectory.getAbsolutePath())
                            .addArgument(SymlinkCLI.SLEEPDURATION, "3");
                    CondorJob job = builder.build();
                    logger.info(job.toString());
                    graph.addVertex(job);
                    continue;
                }

                String format = "%s,%s";
                List<String> targetLinkPairList = new ArrayList<String>();

                Set<FileData> sampleFileDatas = sample.getFileDatas();
                for (FileData fd : sampleFileDatas) {

                    File f = new File(fd.getPath(), fd.getName());

                    if (fd.getMimeType().equals(MimeType.FASTQ) && fd.getName().endsWith("_R1.fastq.gz")) {
                        if (!f.exists()) {
                            throw new WorkflowException(String.format("File does not exist: %s", f.getAbsolutePath()));
                        }
                        File symlink = new File(sequenceProjDirectory, fd.getName());
                        targetLinkPairList.add(String.format(format, f.getAbsolutePath(), symlink.getAbsolutePath()));
                    }

                    if (fd.getMimeType().equals(MimeType.FASTQ) && fd.getName().endsWith("_R2.fastq.gz")) {
                        if (!f.exists()) {
                            throw new WorkflowException(String.format("File does not exist: %s", f.getAbsolutePath()));
                        }
                        File symlink = new File(sequenceProjDirectory, fd.getName());
                        targetLinkPairList.add(String.format(format, f.getAbsolutePath(), symlink.getAbsolutePath()));
                    }

                    if (fd.getMimeType().equals(MimeType.APPLICATION_BAM) && fd.getName().endsWith(".fixed-rg.bam")) {
                        if (!f.exists()) {
                            throw new WorkflowException(String.format("File does not exist: %s", f.getAbsolutePath()));
                        }
                        File symlink = new File(alignmentProjDirectory, fd.getName());
                        targetLinkPairList.add(String.format(format, f.getAbsolutePath(), symlink.getAbsolutePath()));
                    }

                    if (fd.getMimeType().equals(MimeType.APPLICATION_BAM_INDEX)
                            && fd.getName().endsWith(".fixed-rg.bai")) {
                        if (!f.exists()) {
                            throw new WorkflowException(String.format("File does not exist: %s", f.getAbsolutePath()));
                        }
                        File symlink = new File(alignmentProjDirectory, fd.getName());
                        targetLinkPairList.add(String.format(format, f.getAbsolutePath(), symlink.getAbsolutePath()));
                    }

                    if (fd.getMimeType().equals(MimeType.TEXT_STAT_SUMMARY)
                            && fd.getName().endsWith(".realign.fix.pr.flagstat")) {
                        if (!f.exists()) {
                            throw new WorkflowException(String.format("File does not exist: %s", f.getAbsolutePath()));
                        }
                        File symlink = new File(alignmentProjDirectory, fd.getName());
                        targetLinkPairList.add(String.format(format, f.getAbsolutePath(), symlink.getAbsolutePath()));
                    }

                }

                String r1ZipFileName = String.format("%s_%s_L%03d_R1.fastqc.zip", sample.getFlowcell().getName(),
                        sample.getBarcode(), sample.getLaneIndex());
                File r1FastQCZipFile = new File(necAlignmentDirectory, r1ZipFileName);
                if (!r1FastQCZipFile.exists()) {
                    throw new WorkflowException(String.format("File does not exist: %s",
                            r1FastQCZipFile.getAbsolutePath()));
                }
                File r1FastQCZipSymlink = new File(sequenceProjDirectory, r1ZipFileName);
                targetLinkPairList.add(String.format(format, r1FastQCZipFile.getAbsolutePath(),
                        r1FastQCZipSymlink.getAbsolutePath()));

                String r2ZipFileName = String.format("%s_%s_L%03d_R1.fastqc.zip", sample.getFlowcell().getName(),
                        sample.getBarcode(), sample.getLaneIndex());
                File r2FastQCZipFile = new File(necAlignmentDirectory, r2ZipFileName);
                if (!r2FastQCZipFile.exists()) {
                    throw new WorkflowException(String.format("File does not exist: %s",
                            r2FastQCZipFile.getAbsolutePath()));
                }
                File r2FastQCZipSymlink = new File(sequenceProjDirectory, r2ZipFileName);
                targetLinkPairList.add(String.format(format, r2FastQCZipFile.getAbsolutePath(),
                        r2FastQCZipSymlink.getAbsolutePath()));

                // cycle through all files in the necVariantCallingDirectory
                for (File f : necVariantCallingDirectory.listFiles()) {
                    if (f.getName().contains(".coverage.")) {
                        File targetFile = new File(alignmentStatProjDirectory, f.getName());
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));
                    }
                }

                String ecTSVFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fix.pr.ec.tsv", sample
                        .getFlowcell().getName(), sample.getBarcode(), sample.getLaneIndex());
                File ecTSVFile = new File(necIDCheckDirectory, ecTSVFileName);
                if (!ecTSVFile.exists()) {
                    throw new WorkflowException(String.format("File does not exist: %s", ecTSVFile.getAbsolutePath()));
                }
                File ecTSVSymlink = new File(idchkFVCFProjDirectory, ecTSVFileName);
                targetLinkPairList.add(String.format(format, ecTSVFile.getAbsolutePath(),
                        ecTSVSymlink.getAbsolutePath()));

                String fVCFFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fix.pr.fvcf", sample
                        .getFlowcell().getName(), sample.getBarcode(), sample.getLaneIndex());
                File fVCFFile = new File(necIDCheckDirectory, fVCFFileName);
                if (!fVCFFile.exists()) {
                    throw new WorkflowException(String.format("File does not exist: %s", fVCFFile.getAbsolutePath()));
                }
                File fVCFSymlink = new File(idchkFVCFProjDirectory, ecTSVFileName);
                targetLinkPairList
                        .add(String.format(format, fVCFFile.getAbsolutePath(), fVCFSymlink.getAbsolutePath()));

                // new job
                CondorJobBuilder builder = WorkflowJobFactory
                        .createJob(++count, BatchSymlinkCLI.class, attempt.getId()).siteName(siteName);
                for (String targetLinkPair : targetLinkPairList) {
                    builder.addArgument(BatchSymlinkCLI.TARGETLINKPAIR, targetLinkPair);
                }
                CondorJob symlinkJob = builder.build();
                logger.info(symlinkJob.toString());
                graph.addVertex(symlinkJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }
}
