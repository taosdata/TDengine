package com.taosdata.tsync;

import com.taosdata.tsync.entity.consumer.ConsumerConfig;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import com.taosdata.tsync.serializer.SerializeIgnore;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TQueueConsumerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, "master");
        props.setProperty(ConsumerConfig.PORT_CONFIG, "6041");
        props.setProperty(ConsumerConfig.USER_CONFIG, "root");
        props.setProperty(ConsumerConfig.PASSWORD_CONFIG, "taosdata");

        TQueueConsumer consumer = new TQueueConsumer(props);
        consumer.assign("tq_test", 1);

        long count = 0;

        while (true) {
            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                String value = new String(record.value(), "UTF-8");
                System.out.printf("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, value);
                count++;
            }
        }
    }

    class Person {
        private String name;
        private int age;
        private Long height;
        private float salary;
        private Double weight;
        private boolean gender;
        private byte[] comment;
        private String introduction;
        @SerializeIgnore
        private boolean sex;

        public Person(String name, Integer age, boolean sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex=" + sex +
                    '}';
        }

        public Long getHeight() {
            return height;
        }

        public void setHeight(Long height) {
            this.height = height;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Float getSalary() {
            return salary;
        }

        public void setSalary(Float salary) {
            this.salary = salary;
        }

        public Double getWeight() {
            return weight;
        }

        public void setWeight(Double weight) {
            this.weight = weight;
        }

        public boolean isGender() {
            return gender;
        }

        public void setGender(boolean gender) {
            this.gender = gender;
        }

        public byte[] getComment() {
            return comment;
        }

        public void setComment(byte[] comment) {
            this.comment = comment;
        }

        public String getIntroduction() {
            return introduction;
        }

        public void setIntroduction(String introduction) {
            this.introduction = introduction;
        }

        public boolean isSex() {
            return sex;
        }

        public void setSex(boolean sex) {
            this.sex = sex;
        }
    }

}
