package com.tsb.utils;

import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.SimpleMailMessage;

import java.io.FileInputStream;
import java.util.Properties;

public class MailUtil {

    public static JavaMailSenderImpl createMailSender(String propPath) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propPath)) {
            props.load(fis);
        }
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost(props.getProperty("mail.host"));
        mailSender.setPort(Integer.parseInt(props.getProperty("mail.port")));
        mailSender.setUsername(props.getProperty("mail.username"));
        mailSender.setPassword(props.getProperty("mail.password"));
        mailSender.setJavaMailProperties(props);
        return mailSender;
    }

    public static void sendMail(String propPath, String to, String subject, String content) throws Exception {
        JavaMailSenderImpl mailSender = createMailSender(propPath);
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(to);
        message.setSubject(subject);
        message.setText(content);
        message.setFrom(mailSender.getUsername());
        mailSender.send(message);
    }
}