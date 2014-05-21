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
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.module.core.BatchSymlinkCLI;
import edu.unc.mapseq.module.core.SymlinkCLI;
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class NECSymlinkWorkflow extends AbstractWorkflow {

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

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String saHome = getWorkflowBeanService().getAttributes().get("sequenceAnalysisHome");

        File cohortDirectory = new File(saHome, "NEC");

        logger.info("cohortDirectory.getAbsolutePath(): {}", cohortDirectory.getAbsolutePath());

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            logger.info("htsfSample: {}", htsfSample.toString());

            String subjectName = null;
            boolean qcPass = false;

            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                EntityAttribute attribute = attributeIter.next();
                String name = attribute.getName();
                String value = attribute.getValue();
                if ("subjectName".equals(name)) {
                    subjectName = value;
                }
                if ("qcPass".equals(name) && StringUtils.isNotEmpty(value) && "true".equalsIgnoreCase(value)) {
                    qcPass = true;
                }
            }

            if (StringUtils.isEmpty(subjectName)) {
                throw new WorkflowException("invalid subjectName");
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                    getName().replace("Symlink", ""), getVersion());
            logger.info("outputDirectory.getAbsolutePath() = {}", outputDirectory.getAbsolutePath());

            List<File> readPairList = WorkflowUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());

            // error check
            if (readPairList.size() != 2) {
                logger.warn("readPairList.size(): {}", readPairList.size());
                throw new WorkflowException("Read pair not found");
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

                // don't link all files, just the analysis directory
                if (!qcPass) {

                    CondorJob job = WorkflowJobFactory.createJob(++count, SymlinkCLI.class, getWorkflowPlan(),
                            htsfSample);
                    job.setSiteName(siteName);
                    job.addArgument(SymlinkCLI.TARGET, outputDirectory.getAbsolutePath());
                    job.addArgument(SymlinkCLI.LINK, qcFailProjDirectory.getAbsolutePath());
                    job.addArgument(SymlinkCLI.SLEEPDURATION, "3");
                    graph.addVertex(job);
                    continue;
                }

                String format = "%s,%s";
                List<String> targetLinkPairList = new ArrayList<String>();
                File fastqR1Symlink = new File(sequenceProjDirectory, r1FastqFile.getName());
                targetLinkPairList.add(String.format(format, r1FastqFile.getAbsolutePath(),
                        fastqR1Symlink.getAbsolutePath()));
                File fastqR2Symlink = new File(sequenceProjDirectory, r2FastqFile.getName());
                targetLinkPairList.add(String.format(format, r2FastqFile.getAbsolutePath(),
                        fastqR2Symlink.getAbsolutePath()));

                // cycle through all files in the analysisWorkflowDirectory
                for (File f : outputDirectory.listFiles()) {
                    String fname = f.getName();

                    if (fname.endsWith("fastqc.zip")) {

                        File targetFile = new File(sequenceProjDirectory, fname);
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));

                    } else if (fname.endsWith("fixed-rg.bam") || fname.endsWith("fixed-rg.bai")) {

                        File targetFile = new File(alignmentProjDirectory, fname);
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));

                    } else if (fname.contains(".coverage.") || fname.endsWith("flagstat")) {

                        File targetFile = new File(alignmentStatProjDirectory, fname);
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));

                    } else if (fname.endsWith("fvcf")) {

                        File targetFile = new File(idchkFVCFProjDirectory, fname);
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));

                    } else if (fname.endsWith("ec.tsv")) {

                        File targetFile = new File(idchkECProjDirectory, fname);
                        targetLinkPairList
                                .add(String.format(format, f.getAbsolutePath(), targetFile.getAbsolutePath()));

                    }

                }

                // new job
                CondorJob symlinkJob = WorkflowJobFactory.createJob(++count, BatchSymlinkCLI.class, getWorkflowPlan(),
                        htsfSample);
                symlinkJob.setSiteName(siteName);
                for (String targetLinkPair : targetLinkPairList) {
                    symlinkJob.addArgument(BatchSymlinkCLI.TARGETLINKPAIR, targetLinkPair);
                }
                graph.addVertex(symlinkJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }
}
