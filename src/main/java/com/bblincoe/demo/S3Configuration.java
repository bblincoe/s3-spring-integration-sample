package com.bblincoe.demo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aws.inbound.S3StreamingMessageSource;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.aws.support.S3RemoteFileTemplate;
import org.springframework.integration.aws.support.filters.S3SimplePatternFileListFilter;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transformer.StreamTransformer;

import java.io.File;
import java.io.InputStream;

/**
 * Created by bblincoe on 6/7/17.
 */
@Configuration
public class S3Configuration {

    @Value("${aws.accessKey}")
    private String accessKey;

    @Value("${aws.secretAccessKey}")
    private String secretAccessKey;

    @Value("${aws.region}")
    private String region;

    @Value("${aws.bucket}")
    private String bucket;

    @Value("${aws.bucketPrefix}")
    private String bucketPrefix;

    @Bean
    public AmazonS3 amazonS3() {
        BasicAWSCredentials awsCredentials =
                new BasicAWSCredentials(accessKey, secretAccessKey);
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();
        return client;
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller() {
        return Pollers.fixedRate(5000).get();
    }

    @Bean
    public MessageSource<InputStream> s3InboundStreamingMessageSource() {
        S3StreamingMessageSource messageSource = new S3StreamingMessageSource(new S3RemoteFileTemplate(amazonS3()));
        messageSource.setRemoteDirectory(bucket);
        messageSource.setFilter(new S3SimplePatternFileListFilter(bucketPrefix));
        return messageSource;
    }

    @Bean
    public FileReadingMessageSource fileReader() {
        FileReadingMessageSource reader = new FileReadingMessageSource();
        reader.setDirectory(new File("src/test/resources/int2"));
        return reader;
    }

    @Bean
    public IntegrationFlow uploadFlow() {
        return IntegrationFlows.from(fileReader())
                .handle(new S3MessageHandler(amazonS3(), bucket)).get();
    }

    @Bean
    public IntegrationFlow downloadFlow() {
        return IntegrationFlows.from(s3InboundStreamingMessageSource())
                .transform(new StreamTransformer("UTF-8"))
                .handle(System.out::println)
                .get();
    }

}
