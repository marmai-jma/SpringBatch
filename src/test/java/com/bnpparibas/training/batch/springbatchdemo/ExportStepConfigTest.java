package com.bnpparibas.training.batch.springbatchdemo;

import static org.junit.Assert.assertEquals;

import com.bnpparibas.training.batch.springbatchdemo.config.ExportJobConfig;
import com.bnpparibas.training.batch.springbatchdemo.config.JobCompletionNotificationListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.AssertFile;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = { BatchTestConfiguration.class, ExportJobConfig.class,
        JobCompletionNotificationListener.class })
public class ExportStepConfigTest {

    private static final String EXPECTED_FILE = "src/test/resources/datas/export-expected.csv";
    private static final String OUTPUT_FILE = "target/output/outputfile.csv";

    @Autowired
    private JobLauncherTestUtils testUtils;

    @Test
    public void exportStepWithFileShouldSuccess() throws Exception {
        // Given
        final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters()) //
                .addString("output-file", OUTPUT_FILE) //
                .toJobParameters();
        // When
        final JobExecution jobExec = testUtils.launchStep("export-step", jobParameters);

        // Then
        assertEquals(BatchStatus.COMPLETED, jobExec.getStatus());

        AssertFile.assertFileEquals(new FileSystemResource(EXPECTED_FILE), //
                new FileSystemResource(OUTPUT_FILE));
    }

}