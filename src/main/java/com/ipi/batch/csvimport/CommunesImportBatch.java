package com.ipi.batch.csvimport;


import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.exception.CommuneCSVException;
import com.ipi.batch.exception.NetworkException;
import com.ipi.batch.listener.CommuneCSVImportStepListener;
import com.ipi.batch.model.Commune;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.validation.BindException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

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
    public Job importCsvJob(Step stepHelloWorld, Step stepImportCSV, Step stepImportCSVWithJDBC, Step stepGetMissingCoordinates) {
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(stepHelloWorld)
                .next(stepImportCSV)
                .next(stepImportCSVWithJDBC)
                .next(stepGetMissingCoordinates)
                .end()
                .build();
    }

    @Bean
    public Step stepHelloWorld() {
        return stepBuilderFactory.get("stepHelloWorld")
                .tasklet(helloWorldTasklet())
                .listener(helloWorldTasklet())
                .build();
    }

    @Bean
    public StepExecutionListener communeCSVImportTestListernet() {
        return new CommuneCSVImportStepListener();
    }

    @Bean
    public Step stepImportCSV() {
            return stepBuilderFactory.get("importFile")
                    .<CommuneCSV, Commune> chunk(10)
                    .reader(communesCSVItemReader())
                    .processor(communeCSVToCommuneProcessor())
                    .writer(writerJPA())
                    .faultTolerant()
                    .skipLimit(100)
                    .skip(CommuneCSVException.class)
                    .listener(communeCSVImportTestListernet())
                    .build();

    }
    @Bean
    public Step stepImportCSVWithJDBC() {
        return stepBuilderFactory.get("importFileWithJDBC")
                .<CommuneCSV, Commune>chunk(10)
                .reader(communesCSVItemReader())
                .processor(communeCSVToCommuneProcessor())
                .writer(writerJDBC(null))
                .build();
    }

    @Bean
    public Step stepGetMissingCoordinates() {
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(2000);
        return stepBuilderFactory.get("getMissingCoordinates")
                .<Commune, Commune>chunk(10)
                .reader(communeMissingCoordinatesJpaItemReader())
                .processor(communesMissingCoordinatesItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .retryLimit(5)
                .retry(NetworkException.class)
                .backOffPolicy(policy)

                .build();
    }

    @Bean
    public JpaItemWriter<Commune> writerJPA() {
        return new JpaItemWriterBuilder<Commune>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Commune>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO COMMUNE(code_insee, code_postal, latitude, longitude) VALUES (:codeInsee, :codePostal, :latitude, :longitude)" +
                        "ON DUPLICATE KEY UPDATE nom = c.nom, code_postal=c.code_postal, latitude=c.latitude, longitude=c.longitude")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public CommuneCSVItemProcessor communeCSVToCommuneProcessor(){
        return new CommuneCSVItemProcessor();
    }

    @Bean
    public FlatFileItemReader<CommuneCSV> communesCSVItemReader() {
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
    public JpaPagingItemReader<Commune> communeMissingCoordinatesJpaItemReader() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communeMissingCoordinatesJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }

    @Bean
    public CommunesMissingCoordinatesItemProcessor communesMissingCoordinatesItemProcessor() {
        return new CommunesMissingCoordinatesItemProcessor();
    }


    @Bean
    public Tasklet helloWorldTasklet() {
        return new HelloWorldTasklet();
    }


}
