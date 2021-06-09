package com.taosdata.tsync;

import com.taosdata.tsync.entity.producer.ProducerConfig;
import com.taosdata.tsync.entity.producer.ProducerRecord;
import com.taosdata.tsync.serializer.SerializeIgnore;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TQueueProducerTest {

    private static final String TOPIC = "tq_test";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void test() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, "master");
        props.setProperty(ProducerConfig.PORT_CONFIG, "6041");
        props.setProperty(ProducerConfig.USER_CONFIG, "root");
        props.setProperty(ProducerConfig.PASSWORD_CONFIG, "taosdata");
        props.setProperty(ProducerConfig.CHARSET_CONFIG, "UTF-8");
        props.setProperty(ProducerConfig.LOCALE_CONFIG, "en_US.UTF-8");
        props.setProperty(ProducerConfig.TIMEZONE_CONFIG, "UTC-8");
        props.setProperty(ProducerConfig.SERIALIZER, ProducerConfig.STRING_SERIALIZER);

        TQueueProducer producer = new TQueueProducer(props);

        List<Thread> threads = IntStream.range(1, 11).mapToObj(partition -> new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    ProducerRecord<Person> record = new ProducerRecord(
                            TOPIC,
                            partition,
                            new Person("name_" + i, random.nextInt(), random.nextBoolean())
                    );
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                        System.out.println(metadata);
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        })).collect(Collectors.toList());

        // start threads
        threads.forEach(Thread::start);

        // wait threads
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
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