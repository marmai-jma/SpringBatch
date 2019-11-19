package com.bnpparibas.training.batch.springbatchdemo.config;

import com.bnpparibas.training.batch.springbatchdemo.dto.BookDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.Delimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
public class ExportJobConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportJobConfig.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean(name = "exportJob")
    public Job exportBookJob (final Step exportStep, final JobCompletionNotificationListener listener){
        return jobBuilderFactory.get("export-Job")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(exportStep)
                .end()
                .build();
    }

    @Bean
    public Step exportStep(final JdbcCursorItemReader<BookDto> exportReader,
                           final ItemProcessor<BookDto,BookDto> exportProcessor,
                           final FlatFileItemWriter<BookDto> exportWriter){
        return (Step) stepBuilderFactory.get("export-step")
                .<BookDto,BookDto>chunk(10) //le step attend des méthodes qui ont BookDTO dans leur signature
                .reader(exportReader)
                .processor(exportProcessor)
                .writer(exportWriter)
                .build();
    }

    //exportReader
    @Bean
    public JdbcCursorItemReader<BookDto> exportReader (){
        final JdbcCursorItemReader<BookDto> reader = new JdbcCursorItemReader<BookDto>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT title, author, isbn, publisher, year FROM Book");
        reader.setRowMapper(new BookRowMapper());
        return reader;
    }



    //exportProcessor
    @Bean
    public ItemProcessor<BookDto, BookDto> exportProcessor() {
        return new ItemProcessor<BookDto, BookDto>() {
            @Override
            public BookDto process(final BookDto book) throws Exception {
                LOGGER.info("Processing {}", book);
                return book;
            }
        };
    }

    // exportWriter
    @StepScope // Mandatory for using jobParameters​- sans cela, pas possible de passer des paramètres
    @Bean
    public FlatFileItemWriter<BookDto> exportWriter(@Value("#{jobParameters['output-file']}") final String ouputFile) {
        return new FlatFileItemWriterBuilder<BookDto>()
                .name("bookItemWriter") //​
                .resource(new FileSystemResource(ouputFile)) // ressource fichier
                .lineAggregator(new DelimitedLineAggregator<BookDto>(){
                    {
                        setDelimiter(",");
                        setFieldExtractor(new BeanWrapperFieldExtractor<BookDto>(){
                            {
                                setNames(new String[]{"title", "author", "isbn", "publisher", "publishedOn"});
                            }
                        });
                    }
                }
                        ).build();
    }

    private class BookRowMapper implements RowMapper<BookDto> {
        @Override
        public BookDto mapRow(final ResultSet resultSet, final int i) throws SQLException { //ResultSet encapsule le resultat d'une requete SQL
            // final bonne pratique pour indiquer que les variables ne sont pas modifiées par la méthode
            BookDto bookDto  = new BookDto();
            bookDto.setTitle(resultSet.getString("title"));  // on peut aussi ecrire getString ("title") avec le nom de colonne
            bookDto.setAuthor(resultSet.getString("author")); //mettre le rang dans la table en commençant par 0 pour 1ere colonne
            bookDto.setIsbn(resultSet.getString("isbn"));
            bookDto.setPublisher(resultSet.getString("publisher"));
            bookDto.setPublishedOn(resultSet.getInt("publishedOn"));
            return bookDto;
        }
    }

}
