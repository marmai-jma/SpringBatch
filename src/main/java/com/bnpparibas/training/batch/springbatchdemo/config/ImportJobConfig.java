package com.bnpparibas.training.batch.springbatchdemo.config;

import com.bnpparibas.training.batch.springbatchdemo.dto.BookDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class ImportJobConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobConfig.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MaClasseMetier maClasseMetier;

    @Bean(name = "importJob") // déclarer un JOB - sera appelé par le framework via le jobLaucher qui cherchera
        // tous les job qu'il a dans son repository
    public Job importBookJob (final Step importStep, final JobCompletionNotificationListener listener){
        return jobBuilderFactory.get("import-Job") //import-job, nom du job à lancer par le jobLaucher (pas celui de la classe)
                .incrementer(new RunIdIncrementer())// crée des instances
                .listener(listener) // facultatif - permet d'insérer du code avant et après le job -
                                    // le listener a 2 methodes beforeJob et afterJob qu'on pourra overrider
                                    // le listener aurait pu être déclaré en autowired comme plus haut
                .flow(importStep)  // les steps (si on en a plusieurs on les empile dans l'ordre d'execution souhaitée
                .end() // on dit que c'est fini
                .build();  // c'est la methode de la factory qui renvoie un job
    }

    @Bean
    public Step importStep(ItemReader<BookDto> importReader,
                           final ItemProcessor<BookDto,BookDto> importProcessor, // injecté le Bean par Spring
                           final ItemWriter<BookDto> importWriter){
        return (Step) stepBuilderFactory.get("import-step")
                .<BookDto,BookDto>chunk(10) //le step attend des méthodes qui ont BookDTO dans leur signature
                .reader(importReader)
                .processor(importProcessor) // on fait .procesor sur le bean importProcessor, il va executer le process qui est dans importProcessor
                .writer(importWriter)
                .build();
    }

    // ici on passe un paramètre pour le chemin parce qu'il peut varier d'une machine à l'autre.
    //Il faut le passer en @Value => SPRING injecte la valeur du paramètre passé.
    //La methode doit être connue de Spring, il faut donc que ce soit un @Bean
    //bookItemReader
    @StepScope // Mandatory for using jobParameters​- sans cela, pas possible de passer des paramètres
    @Bean
    public FlatFileItemReader<BookDto> importReader(@Value("#{jobParameters['input-file']}") final String inputFile) {
        return new FlatFileItemReaderBuilder<BookDto>()
                .name("bookItemReader") //​
                .resource(new FileSystemResource(inputFile)) // ressource fichier
                .delimited() // fichier csv
                .delimiter(";") // delimiteur ;
                .names(new String[] { "title", "author", "isbn", "publisher","publishedOn" })
                // dit dans quel ordre on va trouver les données de DTO dans le fichier
                .linesToSkip(1) // on saute la première ligne parce qu'elle contient les entêtes - on ne verifie pas cela
                .fieldSetMapper(new BeanWrapperFieldSetMapper<BookDto>() {
                    {
                        setTargetType(BookDto.class);
                    }
                }).build();
    }

    //importProcessor
    @Bean
    public ItemProcessor<BookDto, BookDto> importProcessor() {
        return new ItemProcessor<BookDto, BookDto>() {
            @Override
            public BookDto process(final BookDto book) throws Exception {
                LOGGER.info("Processing {}", book);
                return maClasseMetier.maMethodeMetier(book);
            }
        };
    }

    //importItemWriter

}
