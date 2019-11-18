package com.bnpparibas.training.batch.springbatchdemo;

import com.bnpparibas.training.batch.springbatchdemo.config.ImportJobConfig;
import com.bnpparibas.training.batch.springbatchdemo.config.JobCompletionNotificationListener;
import com.bnpparibas.training.batch.springbatchdemo.config.MaClasseMetier;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.test.jdbc.JdbcTestUtils.deleteFromTables;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {BatchTestConfiguration.class, ImportJobConfig.class,
        JobCompletionNotificationListener.class, MaClasseMetier.class})
public class ImportStepConfigTest {
    @Autowired
    private JobLauncherTestUtils testUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Before
    public void deleteBooks(){
        deleteFromTables(jdbcTemplate,"book");
    }

    @Test
    public void importJob() throws Exception {
        //Given
        final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters())
                .addString("input-file","src/main.resources/sample-data.csv")
                .toJobParameters();
        //when
        final JobExecution jobExec = testUtils.launchJob(jobParameters);
        // then
        assertThat(jobExec.getStatus(), equalTo(BatchStatus.COMPLETED));
    }
}
