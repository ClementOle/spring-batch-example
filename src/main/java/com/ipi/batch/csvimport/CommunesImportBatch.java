package com.ipi.batch.csvimport;


import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.model.Commune;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

@Configuration
@EnableBatchProcessing
public class CommunesImportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Bean
    public JpaItemWriter<Commune> writerJPA() {
        return new JpaItemWriterBuilder<Commune>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public CommuneCSVItemProcessor communeCSVToCommuneProcessor(){
        return new CommuneCSVItemProcessor();
    }

    @Bean
    public FlatFileItemReader<CommuneCSV> communeCSVItemReader() {
        return new FlatFileItemReaderBuilder<CommuneCSV>()
                .name("communesCSVItemReader")
                .linesToSkip(1)
                .resource(new ClassPathResource("laposte_hexasmal.csv"))
                .delimited().delimiter(";")
                .names("codeInsee", "nom", "codePostal", "ligne5", "libelleAcheminement", "coordonneesGPS")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<CommuneCSV>() {{
                    setTargetType(CommuneCSV.class);
                }})
                .build();
    }


    @Bean
    public Job importCsvJob(Step stepHelloWorld) {
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(stepHelloWorld)
                .end()
                .build();
    }

    @Bean
    public Step stepHelloWorld() {
        return stepBuilderFactory.get("stepHelloWorld")
                .tasklet(helloWorldTasklet())
                .build();
    }

    @Bean
    public Tasklet helloWorldTasklet() {
        return new HelloWorldTasklet();
    }


}
