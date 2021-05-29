package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.dbexport.callback.FooterCallback;
import com.ipiecoles.batch.dbexport.callback.HeaderCallback;
import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;

import java.util.Map;

@Configuration
@EnableBatchProcessing
public class CommunesDBExportBatch {

    @Autowired
    private CommuneRepository communeRepository;

    @Value("${importFile.chunkSize}")
    private Integer chunkSize;


    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(final JobBuilderFactory jobBuilderFactory, Step stepExportCommunes) {
        return jobBuilderFactory.get("exportCommunes")
                .start(stepExportCommunes)
                .build();
    }


    @Bean
    public Step stepExportCommunes(final StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("stepExportCommunes")
                .<Commune, Commune>chunk(chunkSize)
                .reader(communesReader())
                .writer(communesWriter())
                .build();
    }

    @Bean
    public RepositoryItemReader<Commune> communesReader() {

        RepositoryItemReader<Commune> rir = new RepositoryItemReader<>();
        rir.setRepository(communeRepository);
        rir.setMethodName("findAll");
        rir.setSort(Map.of("codePostal", Sort.Direction.ASC, "codeInsee", Sort.Direction.ASC));
        rir.setPageSize(10);
        return rir;
    }

    @Bean
    public FlatFileItemWriter<Commune> communesWriter() {
        BeanWrapperFieldExtractor<Commune> wrapper = new BeanWrapperFieldExtractor<>();
        wrapper.setNames(new String[]{"codePostal", "codeInsee", "nom", "latitude", "longitude"});

        FormatterLineAggregator<Commune> agg = new FormatterLineAggregator<>();
        agg.setFormat("%5s - %5s - %s : %.5f %.5f");
        agg.setFieldExtractor(wrapper);

        return new FlatFileItemWriterBuilder<Commune>()
                .name("communesWriter")
                .lineAggregator(agg)
                .headerCallback(new HeaderCallback(communeRepository))
                .footerCallback(new FooterCallback(communeRepository))
                .resource(new FileSystemResource("target/test.txt"))
                .build();
    }
}
