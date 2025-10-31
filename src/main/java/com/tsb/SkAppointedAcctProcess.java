package com.tsb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.tsb.dataimport.DataTransferProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataTransferProperties.class)
public class SkAppointedAcctProcess {

	public static void main(String[] args) {
		SpringApplication.run(SkAppointedAcctProcess.class, args);
	}

}
