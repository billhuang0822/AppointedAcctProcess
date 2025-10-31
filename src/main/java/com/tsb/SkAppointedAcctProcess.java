package com.tsb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.tsb.dataimport.DataTransferProperties;
import com.tsb.test.TestTableProperties;

@SpringBootApplication
@EnableConfigurationProperties({DataTransferProperties.class, TestTableProperties.class})
public class SkAppointedAcctProcess {
    public static void main(String[] args) {
        SpringApplication.run(SkAppointedAcctProcess.class, args);
    }
}